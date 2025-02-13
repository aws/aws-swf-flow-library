/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow;


public class ActivitySchedulingOptions {
    
    private Long heartbeatTimeoutSeconds;
    
    private Long scheduleToCloseTimeoutSeconds;
    
    private Long scheduleToStartTimeoutSeconds;
	
    private Long startToCloseTimeoutSeconds;
	
    private String taskList;
	
    private Integer taskPriority;
    
	public Long getHeartbeatTimeoutSeconds() {
        return heartbeatTimeoutSeconds;
    }

    public void setHeartbeatTimeoutSeconds(Long heartbeatTimeoutSeconds) {
        this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds;
    }
    
    public ActivitySchedulingOptions withHeartbeatTimeoutSeconds(Long heartbeatTimeoutSeconds) {
        this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds;
        return this;
    }
	
	public Long getScheduleToCloseTimeoutSeconds() {
		return scheduleToCloseTimeoutSeconds;
	}
	
	public void setScheduleToCloseTimeoutSeconds(Long scheduleToCloseTimeoutSeconds) {
		this.scheduleToCloseTimeoutSeconds = scheduleToCloseTimeoutSeconds;
	}
	
	public ActivitySchedulingOptions withScheduleToCloseTimeoutSeconds(Long scheduleToCloseTimeoutSeconds) {
		this.scheduleToCloseTimeoutSeconds = scheduleToCloseTimeoutSeconds;
		return this;
	}
	
	public Long getScheduleToStartTimeoutSeconds() {
		return scheduleToStartTimeoutSeconds;
	}
	
	public void setScheduleToStartTimeoutSeconds(Long scheduleToStartTimeoutSeconds) {
		this.scheduleToStartTimeoutSeconds = scheduleToStartTimeoutSeconds;
	}
	
	public ActivitySchedulingOptions withScheduleToStartTimeoutSeconds(Long scheduleToStartTimeoutSeconds) {
		this.scheduleToStartTimeoutSeconds = scheduleToStartTimeoutSeconds;
		return this;
	}
	
	public Long getStartToCloseTimeoutSeconds() {
        return startToCloseTimeoutSeconds;
    }

    public void setStartToCloseTimeoutSeconds(Long startToCloseTimeoutSeconds) {
        this.startToCloseTimeoutSeconds = startToCloseTimeoutSeconds;
    }
    
    public ActivitySchedulingOptions withStartToCloseTimeoutSeconds(Long startToCloseTimeoutSeconds) {
        this.startToCloseTimeoutSeconds = startToCloseTimeoutSeconds;
        return this;
    }
	
	public String getTaskList() {
		return taskList;
	}
	
	public void setTaskList(String taskList) {
		this.taskList = taskList;
	}
	
	public ActivitySchedulingOptions withTaskList(String taskList) {
		this.taskList = taskList;
		return this;
	}
	
    public Integer getTaskPriority() {
        return taskPriority;
    }
	
    public void setTaskPriority(Integer taskPriority) {
        this.taskPriority = taskPriority;
    }
    
    public ActivitySchedulingOptions withTaskPriority(Integer taskPriority) {
    	this.taskPriority = taskPriority;
    	return this;
    }
}
