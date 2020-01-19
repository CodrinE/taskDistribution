Cluster task distribution system using jGroups.

This is a school project that uses the basic concepts of JGroups and then implement a task distribution
system (on top of JGroups), where tasks can be placed into the cluster and are executed by worker nodes.

You will see that worker nodes can be added at run time or taken down, also tasks
assigned to workers who subsequently crash are automatically reassigned to live nodes.