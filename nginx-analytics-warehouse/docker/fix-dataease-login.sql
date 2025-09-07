-- DataEase登录修复SQL脚本
-- 解决Quartz调度器CheckDsStatusJob类缺失导致的登录500错误

USE dataease;

-- 1. 查询问题任务
SELECT 'Before cleanup:' as status;
SELECT COUNT(*) as total_jobs FROM QRTZ_JOB_DETAILS;
SELECT job_name, job_class_name FROM QRTZ_JOB_DETAILS WHERE job_class_name LIKE '%CheckDsStatusJob%';

-- 2. 删除问题任务相关的所有Quartz记录
-- 先删除触发器关联
DELETE FROM QRTZ_TRIGGERS WHERE job_name IN (
    SELECT job_name FROM QRTZ_JOB_DETAILS WHERE job_class_name LIKE '%CheckDsStatusJob%'
);

-- 删除简单触发器
DELETE FROM QRTZ_SIMPLE_TRIGGERS WHERE trigger_name IN (
    SELECT trigger_name FROM QRTZ_TRIGGERS WHERE job_name IN (
        SELECT job_name FROM QRTZ_JOB_DETAILS WHERE job_class_name LIKE '%CheckDsStatusJob%'
    )
);

-- 删除Cron触发器
DELETE FROM QRTZ_CRON_TRIGGERS WHERE trigger_name IN (
    SELECT trigger_name FROM QRTZ_TRIGGERS WHERE job_name IN (
        SELECT job_name FROM QRTZ_JOB_DETAILS WHERE job_class_name LIKE '%CheckDsStatusJob%'
    )
);

-- 删除任务详情
DELETE FROM QRTZ_JOB_DETAILS WHERE job_class_name LIKE '%CheckDsStatusJob%';

-- 3. 验证清理结果
SELECT 'After cleanup:' as status;
SELECT COUNT(*) as remaining_jobs FROM QRTZ_JOB_DETAILS;
SELECT job_name, job_class_name FROM QRTZ_JOB_DETAILS WHERE job_class_name LIKE '%CheckDsStatusJob%';

-- 4. 检查其他可能的问题任务
SELECT 'Other potential problem jobs:' as status;
SELECT job_name, job_class_name FROM QRTZ_JOB_DETAILS WHERE job_class_name NOT LIKE 'io.dataease%';

-- 提交事务
COMMIT;