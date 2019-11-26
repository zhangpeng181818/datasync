package com.zp;

import com.zp.vendor.PeopleHospitalRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@Component
public class ServerRunner {

    @Value("${schedule.cron}")
    private String periodMinute;

    private ScheduledFuture<?> syncHisPatientTaskFuture;

    private ScheduledFuture<?> syncHisOrderTaskFuture;

    private ScheduledFuture<?> syncLisItemsResultTaskFuture;

    private ScheduledFuture<?> syncHisPatientTransferTaskFuture;

    @Autowired
    private PeopleHospitalRunner peopleHospitalRunner;

    @Autowired
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    @PostConstruct
    public void startSyncTask() throws ClassNotFoundException {

        syncHisPatientTaskFuture = threadPoolTaskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    peopleHospitalRunner.startSyncHisPatientsJob();
                } catch (Exception e) {
                    log.error("{}", e.toString(), e);
                }
            }
        }, new Trigger() {
            @Override
            public Date nextExecutionTime(TriggerContext triggerContext) {
                return new CronTrigger(periodMinute).nextExecutionTime(triggerContext);
            }
        });

        syncHisOrderTaskFuture = threadPoolTaskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {

                } catch (Exception e) {
                    log.error("{}", e.toString(), e);
                }
            }
        }, new Trigger() {
            @Override
            public Date nextExecutionTime(TriggerContext triggerContext) {
                return new CronTrigger(periodMinute).nextExecutionTime(triggerContext);
            }
        });


        syncLisItemsResultTaskFuture = threadPoolTaskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    peopleHospitalRunner.startSyncLisItemsResultJob();
                } catch (Exception e) {
                    log.error("{}", e.toString(), e);
                }
            }
        }, new Trigger() {
            @Override
            public Date nextExecutionTime(TriggerContext triggerContext) {
                return new CronTrigger(periodMinute).nextExecutionTime(triggerContext);
            }
        });

        syncHisPatientTransferTaskFuture = threadPoolTaskScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    peopleHospitalRunner.startSyncHisPatientTransfertJob();
                } catch (Exception e) {
                    log.error("{}", e.toString(), e);
                }
            }
        }, new Trigger() {
            @Override
            public Date nextExecutionTime(TriggerContext triggerContext) {
                return new CronTrigger(periodMinute).nextExecutionTime(triggerContext);
            }
        });
    }

    @PreDestroy
    private void shutdown() {
        if (syncHisPatientTaskFuture != null) {
            syncHisPatientTaskFuture.cancel(true);
        }
        if (syncHisOrderTaskFuture != null) {
            syncHisOrderTaskFuture.cancel(true);
        }

        if (syncLisItemsResultTaskFuture != null) {
            syncLisItemsResultTaskFuture.cancel(true);
        }

        if (syncHisPatientTransferTaskFuture != null) {
            syncHisPatientTransferTaskFuture.cancel(true);
        }
    }
}