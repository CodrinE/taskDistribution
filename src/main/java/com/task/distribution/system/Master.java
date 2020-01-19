package com.task.distribution.system;


public interface Master {
    Object submit(Task task, long timeout) throws Exception;
}
