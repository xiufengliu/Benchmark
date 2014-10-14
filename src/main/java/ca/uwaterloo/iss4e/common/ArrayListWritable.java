package ca.uwaterloo.iss4e.common;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;


/**
 *  Copyright (c) 2014 Xiufeng Liu ( xiufeng.liu@uwaterloo.ca )
 *
 *  This file is free software: you may copy, redistribute and/or modify it
 *  under the terms of the GNU General Public License version 2
 *  as published by the Free Software Foundation.
 *
 *  This file is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see http://www.gnu.org/licenses.
 */

public class ArrayListWritable<E extends Writable> extends ArrayList<E> implements Writable {
    private static final long serialVersionUID = 4911321393319821791L;

    /**
     * Creates an ArrayListWritable object.
     */
    public ArrayListWritable() {
        super();
    }

    /**
     * Creates an ArrayListWritable object from an ArrayList.
     */
    public ArrayListWritable(ArrayList<E> array) {
        super(array);
    }

    /**
     * Deserializes the array.
     *
     * @param in source for raw byte representation
     */
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        this.clear();

        int numFields = in.readInt();
        if (numFields == 0)
            return;
        String className = in.readUTF();
        E obj;
        try {
            Class<E> c = (Class<E>) Class.forName(className);
            for (int i = 0; i < numFields; i++) {
                obj = (E) c.newInstance();
                obj.readFields(in);
                this.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Serializes this array.
     *
     * @param out where to write the raw byte representation
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.size());
        if (size() == 0)
            return;
        E obj = get(0);

        out.writeUTF(obj.getClass().getCanonicalName());

        for (int i = 0; i < size(); i++) {
            obj = get(i);
            if (obj == null) {
                throw new IOException("Cannot serialize null fields!");
            }
            obj.write(out);
        }
    }

    /**
     * Generates human-readable String representation of this ArrayList.
     *
     * @return human-readable String representation of this ArrayList
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        for (int i = 0; i < this.size(); i++) {
            if (i != 0)
                sb.append(", ");
            sb.append(this.get(i));
        }
        sb.append("]");

        return sb.toString();
    }
}