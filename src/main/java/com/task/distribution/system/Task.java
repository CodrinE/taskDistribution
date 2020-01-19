package com.task.distribution.system;

import java.io.Serializable;

public interface Task extends Serializable {
    Object executeTask();
}
