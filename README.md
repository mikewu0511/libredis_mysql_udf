libredis_mysql_udf
==================

A high performance mysql udf for pushing data to redis cache

###Dependencies
- hiredis(<https://github.com/redis/hiredis>), This is the official C client for Redis.

###Compile and Install
- Compile:
  - gcc $(mysql_config --cflags) -lhiredis -shared -fPIC -oudf_redis.so udf_redis.c
- Install:
  - copy 'udf_redis.so' file to plugin directory of mysql. You can use the command "show variables like '%plugin_dir%';" to find the directory.
  - CREATE FUNCTION redis_server_set RETURNS INTEGER SONAME "udf_redis.so";
  - CREATE FUNCTION redis_exec RETURNS INTEGER SONAME "udf_redis.so";

###Using Examples
```
select redis_server_set('127.0.0.1', 6379, 'password');
select redis_exec('set', 'test_key', '200');
select redis_exec('hset','test:11:hash','username', 'mike');
...
select redis_exec(<command>,<key_name>);
select redis_exec(<command>,<key_name>,<arg_1>);
select redis_exec(<command>,<key_name>,<arg_1>, <arg_2>);
```

###More
- you may create a trigger on the table which you want to sync data with redis
