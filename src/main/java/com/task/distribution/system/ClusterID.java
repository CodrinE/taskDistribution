package com.task.distribution.system;

import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Getter;

@NoArgsConstructor
@AllArgsConstructor
@Getter
public class ClusterID implements Streamable {
    private Address theCreator;
    private int id;

    private static int next_id = 1;

    public static synchronized ClusterID create(Address address) {
        return new ClusterID(address, next_id++);
    }

    public int hashCode() {
        return theCreator.hashCode() + id;
    }

    public boolean equals(Object o) {
        ClusterID other = (ClusterID)o;
        return theCreator.equals(other.theCreator) && id == other.id;
    }

    public String toString() {
        return theCreator + "::" + id;
    }

    @Override
    public void writeTo(DataOutput dataOutput) throws IOException {
        Util.writeAddress(theCreator, dataOutput);
        dataOutput.writeInt(id);
    }

    @Override
    public void readFrom(DataInput dataInput) throws IOException, ClassNotFoundException {
        theCreator = Util.readAddress(dataInput);
        id = dataInput.readInt();
    }
}
