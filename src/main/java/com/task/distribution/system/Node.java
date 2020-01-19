package com.task.distribution.system;

import org.jgroups.*;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Promise;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

public class Node extends ReceiverAdapter implements Master, Slave {
    private String properties;
    private JChannel channel;
    private final ConcurrentMap<ClusterID,Entry> allTasks = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor thread_pool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    private View view;
    private int rank = -1;
    private int sizeOfCluster = -1;


    public Node(String properties) {
        this.properties = properties;
    }

    public void start(String name) throws Exception {
        channel = new JChannel(properties).name(name).receiver(this);
        channel.connect("task-cluster");
        JmxConfigurator.registerChannel(channel, Util.getMBeanServer(), "group", channel.getClusterName(), true);

    }

    public void stop() throws Exception {
        thread_pool.shutdown();
        JmxConfigurator.unregisterChannel(channel, Util.getMBeanServer(), "group", channel.getClusterName());
        channel.close();
    }

    public String info() {
        return String.format("local_address=%s\nview=%s\nrank=%d\n(%d number of entries in tasks cache)\n",
                             channel.getAddress(), view, rank, allTasks.size());
    }


    public Object submit(Task task, long timeout) throws Exception {
        ClusterID id = ClusterID.create(channel.getAddress());
        try {
            Request request = new Request(Request.Type.EXECUTE, task, id, null);
            byte[] buffer = Util.streamableToByteBuffer(request);
            Entry entry = new Entry(task, channel.getAddress());
            allTasks.put(id, entry);
            log(">>>>>> submitting " + id);
            channel.send(new Message(null, buffer));
            return entry.promise.getResultWithTimeout(timeout);
        }
        catch(Exception ex) {
            allTasks.remove(id);
            throw ex;
        }
    }

    public Object handleTask(Task task) {
        return task.executeTask();
    }

    public void receive(Message message) {
        try {
            Request request = Util.streamableFromByteBuffer(Request.class, message.getRawBuffer(), message.getOffset(), message.getLength());
            switch(request.type) {
                case EXECUTE:
                    handleExecute(request.id, message.getSrc(), request.task);
                    break;
                case RESULT:
                    Entry entry = allTasks.get(request.id);
                    if(entry == null) {
                        error("No entry for the request " + request.id + " was found");
                    }
                    else {
                        entry.promise.setResult(request.result);
                    }
                    multicastRemoveRequest(request.id);
                    break;
                case REMOVE_TASK:
                    allTasks.remove(request.id);
                    break;
                default:
                    throw new IllegalArgumentException("The request type " + request.type + " is not recognized");
            }
        }
        catch(Exception e) {
            error("An exception occurred while receiving message from " + message.getSrc(), e);
        }
    }

    private void multicastRemoveRequest(ClusterID id) {
        Request remove_req = new Request(Request.Type.REMOVE_TASK, null, id, null);
        try {
            byte[] buf = Util.streamableToByteBuffer(remove_req);
            channel.send(new Message(null, buf));
        }
        catch(Exception e) {
            error("Failed multicast REMOVE request", e);
        }
    }

    private void handleExecute(ClusterID id, Address sender, Task task) {
        allTasks.putIfAbsent(id, new Entry(task, sender));
        int index = id.getId() % sizeOfCluster;
        if(index != rank) {
            return;
        }

         log("ID = " + id + ", index = " + index + " , my rank = " + rank);
        execute(id, sender, task);
    }

    private void execute(ClusterID id, Address sender, Task task) {
        Handler handler = new Handler(id, sender, task);
        thread_pool.execute(handler);
    }


    public void viewAccepted(View view) {

        List<Address> member = (this.view != null && view != null)?
          Util.leftMembers(this.view.getMembers(), view.getMembers()) : null;
        this.view = view;

        Address local_address = channel.getAddress();
        log("view: " + view);

        sizeOfCluster = view.size();

        List<Address> membership = view.getMembers();
        int old_rank = rank;

        for(int i = 0; i < membership.size(); i++) {
            Address temp = membership.get(i);
            if(temp.equals(local_address)) {
                rank = i;
                break;
            }
        }
        if(old_rank == -1 || old_rank != rank)
            log("My current rank is " + rank);

        if(member != null && !member.isEmpty()) {
            member.forEach(this::handleMember);
        }
    }

    private void handleMember(Address member) {
        for(Map.Entry<ClusterID,Entry> entry: allTasks.entrySet()) {
            ClusterID id = entry.getKey();
            int index = id.getId() % sizeOfCluster;
            if(index != rank)
                return;
            Entry val = entry.getValue();
            if(member.equals(val.submitter)) {
                error("I will not take over tasks submitted by " + member + " because it left the cluster");
                continue;
            }
            log("I am taking over task " + id + " from " + member + " (submitted by " + val.submitter + ")");
            execute(id, val.submitter, val.task);
        }
    }



    private static void loop(Node node) {
        boolean looping = true;
        while(looping) {
            int key = Util.keyPress("[1] Submit [2] Submit long running task [3] Info [q] Quit");
            switch(key) {
                case '1':
                    submitResult(Date::new, node);
                    break;
                case '2':
                    Task task=() -> {
                        log("Sleeping for 15 secs...");
                        Util.sleep(15000);
                        log("done");
                        return new Date();
                    };
                    submitResult(task, node);
                    break;
                case '3':
                    log(node.info());
                    break;
                case 'q':
                case -1:
                    looping=false;
                    break;
                case 'r':
                case '\n':
                    break;
            }
        }
        try {
            node.stop();
        }
        catch(Exception e) {
            error("Caught exception stopping server: " + e);
        }
    }

    private static void submitResult(Task task, Node node) {
        try {
            Object result = node.submit(task, 30000);
            log("<<<<< result = " + result);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }


    private static void log(String msg) {
        System.out.println(msg);
    }

    private static void error(String msg) {
        System.err.println(msg);
    }

    private static void error(String msg, Throwable t) {
        System.err.println(msg + ", ex=" + t);
    }


    @AllArgsConstructor
    private static class Entry {
        private final Task task;
        private final Address submitter;
        private final Promise<Object> promise = new Promise<>();

    }

    @AllArgsConstructor
    private class Handler implements Runnable {
        final ClusterID id;
        final Address sender;
        final Task task;

        public void run() {
            Object result = null;
            if(task != null) {
                try {
                    log("Executing " + id);
                    result = handleTask(task);
                }
                catch(Throwable t) {
                    error("Failed executing " + id, t);
                    result = t;
                }
            }
            Request response = new Request(Request.Type.RESULT, null, id, result);
            try {
                byte[] buffer = Util.streamableToByteBuffer(response);
                Message resp = new Message(sender, buffer);
                channel.send(resp);
            }
            catch(Exception e) {
                error("Failed executing task " + id, e);
            }
        }
    }
    

    @NoArgsConstructor
    @AllArgsConstructor
    public static class Request implements Streamable {
        enum Type {EXECUTE, RESULT, REMOVE_TASK};

        private Type type;
        private Task task;
        private ClusterID id;
        private Object result;


        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(type.ordinal());
            try {
                Util.objectToStream(task, out);
            }
            catch(Exception e) {
                throw new IOException("Failed marshalling of task " + task, e);
            }
            Util.writeStreamable(id, out);
            try {
                Util.objectToStream(result, out);
            }
            catch(Exception e) {
                throw new IOException("Failed to marshall result object", e);
            }
        }

        @SneakyThrows
        public void readFrom(DataInput in) {
            int tmp = in.readInt();
            switch(tmp) {
                case 0:
                    type = Type.EXECUTE;
                    break;
                case 1:
                    type = Type.RESULT;
                    break;
                case 2:
                    type = Type.REMOVE_TASK;
                    break;
                default:
                    throw new InstantiationException("Ordinal " + tmp + " cannot be mapped to enum");
            }
            try {
                task = Util.objectFromStream(in);
            }
            catch(Exception e) {
                InstantiationException ex=new InstantiationException("Failed to read task from stream");
                ex.initCause(e);
                throw ex;
            }
            id = Util.readStreamable(ClusterID::new , in);
            try {
                result = Util.objectFromStream(in);
            }
            catch(Exception e) {
                throw new IOException("Failed to unmarshal the result object", e);
            }
        }

    }

    public static void main(String[] args) throws Exception {

        String properties = "/home/codec/IdeaProjects/taskDistribution/conf/config.xml";

        Node node = new Node(properties);
        node.start(null);

        loop(node);
    }
    
}
