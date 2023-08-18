### MySQL sink

*This sink supports Mysql versions v5.7 and above*.
To use the Mysql sink add the following flag:

	--sink=databend:?https://username:password@tenant--warehousename.ch.datafusecloud.com/test

*First build table*

```
create database kube_eventer;
use kube_eventer;

CREATE TABLE  kube_eventer.kube_eventer
(
    cluster_name     VARCHAR  DEFAULT '' COMMENT 'cluster name',
    name             VARCHAR  DEFAULT '' COMMENT 'event name',
    namespace        VARCHAR  DEFAULT '' COMMENT 'event namespace',
    event_id         VARCHAR  DEFAULT '' COMMENT 'event_id',
    `type`             VARCHAR  DEFAULT '' COMMENT 'event type Warning or Normal',
    reason           VARCHAR  DEFAULT '' COMMENT 'event reason',
    message          VARCHAR  DEFAULT ''  COMMENT 'event message',
    kind             VARCHAR  DEFAULT '' COMMENT 'event kind',
    first_occurrence_time   VARCHAR    DEFAULT '' COMMENT 'event first occurrence time',
    last_occurrence_time    VARCHAR    DEFAULT '' COMMENT 'event last occurrence time' 
);
```

For example:

    --sink=mysql:?http://username:password@tenant--warehousename.ch.datafusecloud.com/test?table=table_name&cluster_name=cluster_name
