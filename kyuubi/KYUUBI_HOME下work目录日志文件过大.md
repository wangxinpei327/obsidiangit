如果后续希望规范管理，不建议长期依赖软链接，可以直接配置：
`$KYUUBI_HOME/conf/kyuubi-env.sh`
增加：
```
export KYUUBI_WORK_DIR_ROOT=/app/kyuubi/work
```
需注意该目录权限
kyuubi.session.engine.log.timeout 默认PT24H If we use Spark as the engine then the session submit log is the console output of spark-submit. We will retain the session submit log until over the config value.
可以减小留存时间