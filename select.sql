

contact.db

-- 查询所有联系人
SELECT username, local_type, alias, remark, nick_name 
FROM contact
ORDER BY username

-- 查询所有群聊
SELECT username, owner, ext_buffer 
FROM chat_room
ORDER BY username



session.db
-- 查询所有会话 (按排序时间戳排序)
SELECT username, summary, last_timestamp, last_msg_sender, last_sender_display_name 
FROM SessionTable 
ORDER BY sort_timestamp DESC



message_xx.db

--查询自增ID(每次获取保存ID，下次查看看ID有没有更新，有更新的表重新查询数据)
SELECT name, seq 
FROM sqlite_sequence
ORDER BY seq DESC;


--查询聊天记录(每个会话的ID都不一样)
SELECT 
    local_id,
    server_id,
    create_time,
    datetime(
        CASE 
            WHEN length(create_time) > 10 THEN create_time / 1000   -- 13位毫秒时间戳
            ELSE create_time                                       -- 10位秒时间戳
        END, 
        'unixepoch', 
        'localtime'
    ) AS create_time_fmt,
    message_content,
    --source,
    status
FROM Msg_2fe4fa04bd7faa46c1d2be9ceb07e98d
ORDER BY create_time DESC
LIMIT 50;


-- 查询群消息

SELECT * FROM contact Where nick_name like '%系统%'
--46170110988@chatroom -->5041f0863066c7ca7cf6701607e171ed 系统运维-技术交流

SELECT * FROM contact Where nick_name like '%系统%'
SELECT username, local_type, alias, remark, nick_name FROM contact Where username like '%@chatroom%'


-- 获取所有聊天记录
SELECT m.sort_seq, m.server_id, m.local_type, n.user_name, m.create_time, m.message_content, m.packed_info_data, m.status
FROM Msg_2fe4fa04bd7faa46c1d2be9ceb07e98d m
LEFT JOIN Name2Id n ON m.real_sender_id = n.rowid
ORDER BY m.create_time DESC

select * from contact where username = 'wxid_e76x515e3ksb12'
select * from contact where username = 'wxid_ger718859jfq21'
select * from contact where username = 'wxid_7t9azqe51qyg22'
select count(*) from contact