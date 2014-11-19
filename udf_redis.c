#include "udf_redis.h"

char *redis_hostname = NULL;
int redis_port = 6379;
char *redis_passwd = NULL;

conn_pool_st conn_pool = {0, NULL};
conn_pool_st *conn_pool_p = &conn_pool;

#ifdef HAVE_DLOPEN

pthread_mutex_t _conn_param_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t _conn_pool_lock = PTHREAD_MUTEX_INITIALIZER;

my_bool redis_exec_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void redis_exec_deinit(UDF_INIT *initid);
longlong redis_exec(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
          char *error);
my_bool redis_server_set_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
longlong redis_server_set(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
          char *error);
void redis_server_set_deinit(UDF_INIT *initid);
void _log_error(char *message);


void _log_error(char *message)
{
   openlog("mysql-redis-udf-err:", LOG_CONS|LOG_PID, LOG_LOCAL0);
   syslog(LOG_ERR, "%s", message);
   closelog();

   return;
}


longlong redis_server_set(UDF_INIT *initid __attribute__((unused)), UDF_ARGS *args,
                    char *is_null __attribute__((unused)),
                    char *error __attribute__((unused)))
{
   pthread_mutex_lock(&_conn_param_lock);

   redis_port = *((longlong*) args->args[1]);
   if(redis_hostname) {
      free(redis_hostname);
   }
   if(redis_passwd) {
      free(redis_passwd);
   }

   redis_hostname = (char*)malloc(args->lengths[0] + 1);
   redis_passwd = (char*)malloc(args->lengths[2] + 1);
   if(redis_hostname == NULL || redis_passwd == NULL) {
      pthread_mutex_unlock(&_conn_param_lock);
      return 1;
   }

   memcpy(redis_hostname, args->args[0], args->lengths[0]);
   memcpy(redis_passwd, args->args[2], args->lengths[2]);
   redis_hostname[args->lengths[0]] = 0;
   redis_passwd[args->lengths[2]] = 0;

   pthread_mutex_unlock(&_conn_param_lock);

   return 0;
}


my_bool redis_server_set_init(UDF_INIT *initid __attribute__((unused)),
                        UDF_ARGS *args __attribute__((unused)),
                        char *message __attribute__((unused)))
{
   if (args->arg_count != 3 || args->arg_type[0] != STRING_RESULT
         || args->arg_type[1] != INT_RESULT || args->arg_type[2] != STRING_RESULT)
   {
      strmov(message, "This function takes arguments:string,int,string");
      return 1;
   }

   return 0;
}


void redis_server_set_deinit(UDF_INIT *initid)
{
   return; 
}


longlong redis_exec(UDF_INIT *initid __attribute__((unused)), UDF_ARGS *args,
                    char *is_null __attribute__((unused)),
                    char *error __attribute__((unused)))
{
   char *command_buff = NULL;
   char *cmd_params[MAX_PARAM_CNT - 1] = {NULL};
   char error_msg[100];
   longlong result = 0;
   int arg_count = args->arg_count - 1;
   int arg_index;

   while(1) {
      if (initid->ptr == 0) {
         result = time(NULL);
         sprintf(error_msg, "[%ld] - redis connect or auth fail", result);
         break;
      }
      command_buff = (char*)malloc(args->lengths[0] + (MAX_PARAM_CNT - 1) * PLACEHOLDER + 1);
      if(command_buff == NULL) {
         result = time(NULL);
         sprintf(error_msg, "[%ld] - command_buff malloc() errno:%d", result, errno);
         break;
      }
      memcpy(command_buff, args->args[0], args->lengths[0]);
      command_buff[args->lengths[0]] = 0;

      int flag = 0;
      for(arg_index = 1; arg_index <= arg_count; arg_index++) {
         cmd_params[arg_index-1] = (char*)malloc(args->lengths[arg_index] + 1);
         if(cmd_params[arg_index-1] == NULL) {
            result = time(NULL);
            sprintf(error_msg, "[%ld] - cmd_params[%d] malloc() errno:%d", result, arg_index-1, errno);
            flag = 1;
            break;
         }
         memcpy(cmd_params[arg_index-1], args->args[arg_index], args->lengths[arg_index]);
         cmd_params[arg_index-1][args->lengths[arg_index]] = 0;
      }
      if(flag) {
         break;
      }

      conn_item_st *conn_item_p = (conn_item_st *)(initid->ptr);
      redisContext *c = (redisContext *)(conn_item_p->c);
      redisReply *reply = NULL;
      if(0 == arg_count) {
         reply = redisCommand(c, command_buff);
      } else if(1 == arg_count) {
         strcat(command_buff, " %s");
         reply = redisCommand(c, command_buff, cmd_params[0]);
      } else if(2 == arg_count) {
         strcat(command_buff, " %s %s");
         reply = redisCommand(c, command_buff, cmd_params[0], cmd_params[1]);
      } else if(3 == arg_count) {
         strcat(command_buff, " %s %s %s");
         reply = redisCommand(c, command_buff, cmd_params[0], cmd_params[1], cmd_params[2]);
      }
      
      if(reply) {
         if(REDIS_REPLY_ERROR == reply->type) {
            result = time(NULL);
            sprintf(error_msg, "[%ld] - exec fail:%s", result, reply->str);
         }
         freeReplyObject(reply);
      } else {
         result = time(NULL);
         sprintf(error_msg, "[%ld] - connect interrupt", result);
         conn_item_p->interrupt = 1;
      }
      
      break;
   }

   if(command_buff) {
      free(command_buff);
   }
   for(arg_index = 0; arg_index < arg_count; arg_index++) {
      if(cmd_params[arg_index]) {
         free(cmd_params[arg_index]);
      }
   }
   
   if(result) {
      _log_error(error_msg);
   }

   return result;
}


my_bool redis_exec_init(UDF_INIT *initid __attribute__((unused)),
                        UDF_ARGS *args __attribute__((unused)),
                        char *message __attribute__((unused)))
{
   if (args->arg_count == 0 || args->arg_count > MAX_PARAM_CNT) {
      strmov(message, "This function takes only 1~4 arguments");
      return 1;
   } else {
      int index;
      for(index = 0; index < args->arg_count; index++) {
         if(args->arg_type[index] != STRING_RESULT) {
            strmov(message, "This function takes only string arguments");
            return 1;
         }
      }

      initid->ptr= (char*)0;

      conn_item_st *conn_item_p;
      pthread_mutex_lock(&_conn_pool_lock);
      if(conn_pool_p->free_count > 0) {
         conn_item_p = conn_pool_p->free_head;
         conn_pool_p->free_head = conn_item_p->next;
         conn_pool_p->free_count--;
         pthread_mutex_unlock(&_conn_pool_lock);
         initid->ptr = (char*)conn_item_p;
      } else {
         pthread_mutex_unlock(&_conn_pool_lock);

         redisContext *c;
         struct timeval timeout = { 1, 500000 }; // 1.5 seconds
         if(redis_hostname == NULL) {
            redis_hostname = "127.0.0.1";
         }
         if(redis_passwd == NULL) {
            redis_passwd = "";
         }
         c = redisConnectWithTimeout(redis_hostname, redis_port, timeout);
         if (c == NULL || c->err) {
            if (c) {
               redisFree(c);
            }
         } else {
            redisReply *reply = NULL;
            reply = redisCommand(c, "AUTH %s", redis_passwd);
            if(reply) {
               if(REDIS_REPLY_STATUS == reply->type) {
                  conn_item_p = (conn_item_st*)malloc(sizeof(conn_item_st));
                  if(conn_item_p) {
                     conn_item_p->c = c;
                     conn_item_p->next = NULL;
                     conn_item_p->interrupt = 0;
                     initid->ptr = (char*)conn_item_p;
                  }
               }
               freeReplyObject(reply);
            }
            
         }
      }
      
   }

   return 0;
}

void redis_exec_deinit(UDF_INIT *initid)
{  
   if(initid->ptr) {
      conn_item_st *conn_item_p = (conn_item_st *)(initid->ptr);
      pthread_mutex_lock(&_conn_pool_lock);
      if(conn_item_p->interrupt || conn_pool_p->free_count > MAX_FREE_CONN) {
         int current_free_conn_cnt = conn_pool_p->free_count;
         pthread_mutex_unlock(&_conn_pool_lock);
         redisContext *c = (redisContext *)(conn_item_p->c);
         redisFree(c);
         free(conn_item_p);

         char error_msg[100];
         sprintf(error_msg, "connect interrupt OR (%d)reach MAX_FREE_CONN!", current_free_conn_cnt);
         _log_error(error_msg);
      } else {
         conn_item_p->next = conn_pool_p->free_head;
         conn_pool_p->free_head = conn_item_p;
         conn_pool_p->free_count++;
         pthread_mutex_unlock(&_conn_pool_lock);
      }
      
   }

   return; 
}



#endif /* HAVE_DLOPEN */
