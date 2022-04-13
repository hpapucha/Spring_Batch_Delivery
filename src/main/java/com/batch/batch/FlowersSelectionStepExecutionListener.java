package com.batch.batch;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class FlowersSelectionStepExecutionListener implements StepExecutionListener {
    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.println("Executing before step logic");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.println("Executing after step logic");
        String flowerType = stepExecution.getJobParameters().getString("type");
        return flowerType.equalsIgnoreCase("roses") ? new ExitStatus("TRIM REQUIRED"):new ExitStatus("NO TRIM REQUIRED");
        //Checks if flowers are roses if they are trim is required
    }
}
