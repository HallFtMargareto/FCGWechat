SELECT
datetime(create_time, 'unixepoch', 'localtime') AS formatted_time, 
    * 
FROM Msg_45e533703db0a2bbad10b4ee54705c8b ORDER BY create_time DESC;