package tech.zjc.arrow.tutorial;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.HashMap;
import java.util.Map;


public class HelloValueVector {

    public static void main(String[] args) {
        testStringVector();
        testIntVector();
        testField();
    }

    public static void testStringVector() {
        try(
            BufferAllocator allocator = new RootAllocator();
            VarCharVector varCharVector = new VarCharVector("variable-size-primitive-layout", allocator);
        ) {
            varCharVector.allocateNew(3);
            varCharVector.set(0, "one".getBytes());
            varCharVector.set(1, "two".getBytes());
            varCharVector.set(2, "three".getBytes());
            varCharVector.setValueCount(3);
            System.out.println("Vector created in memory: " + varCharVector);
        }
    }


    public static void testIntVector() {
        try(
            BufferAllocator allocator = new RootAllocator();
            IntVector intVector = new IntVector("fixed-size-primitive-layout", allocator);
        ) {
            // Must have sufficient space to store values
            intVector.allocateNew(3);
            intVector.set(0, 1);
            intVector.setNull(1);
            intVector.set(2, 2);
            // Programmers should set value count. Otherwise, reader and write will not be able to
            // read and write values
            intVector.setValueCount(3);
            System.out.println("Vector created in memory: " + intVector);
        }
    }

    public static void testField() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("A", "Id card");
        metadata.put("B", "Passport");
        metadata.put("C", "Visa");
        Field document = new Field("document",
                new FieldType(true, new ArrowType.Utf8(), /*dictionary*/ null, metadata),
                /*children*/ null);
        System.out.println("Field created: " + document + ", Metadata: " + document.getMetadata());
    }

}
