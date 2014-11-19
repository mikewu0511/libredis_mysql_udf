#include <my_global.h>
#include <my_sys.h>

#if defined(MYSQL_SERVER)
#include <m_string.h>
#else
#include <string.h>
#define strmov(a,b) stpcpy(a,b)
#endif

#include <mysql.h>
#include <ctype.h>
#include <syslog.h>
#include <hiredis/hiredis.h>

#define MAX_PARAM_CNT 4
#define PLACEHOLDER 3
#define MAX_FREE_CONN 50

typedef struct conn_item conn_item_st;
typedef struct conn_pool conn_pool_st;

struct conn_item
{
	redisContext *c;
	conn_item_st *next;
	my_bool interrupt;
};

struct conn_pool
{
	int free_count;
	conn_item_st *free_head;
};
