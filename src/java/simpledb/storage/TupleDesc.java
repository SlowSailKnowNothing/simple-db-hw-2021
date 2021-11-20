package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;//比如int类，string类
        
        /**
         * The name of the field
         * */
        public final String fieldName; //字段的名字，比如student，teacher

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private List<TDItem> listOfTDItem;
    private   int fieldLength;
    private   int fieldSize;




    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return listOfTDItem.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry. 指定字段的类型和数量，至少有一个entry
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null. 指定字段的名字，可以为空
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.fieldSize=0;
        listOfTDItem=new ArrayList<>();
        this.fieldLength=typeAr.length;
        for(int i=0;i<fieldLength;i++){
            Type temp=typeAr[i];
            this.fieldSize+=temp.getLen();
            listOfTDItem.add(new TDItem(temp,fieldAr[i]));//这里不确定会不会超出String的下标索引，出错了再看看
        }
        
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        listOfTDItem=new ArrayList<>();
        this.fieldLength=typeAr.length;
        for(int i=0;i<fieldLength;i++){
            Type temp=typeAr[i];
            fieldSize+=temp.getLen();
            listOfTDItem.add(new TDItem(temp,"Null"));//这里不确定会不会超出String的下标索引，出错了再看看
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldLength;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i>=fieldLength) throw new NoSuchElementException();
        else return listOfTDItem.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i>=fieldLength) throw new NoSuchElementException();
        else return listOfTDItem.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        int i=0;
        try {
            for(;i<fieldLength;i++){
                if(listOfTDItem.get(i).fieldName.equals(name))return i;
            }
        } catch (Exception e) {}
            throw new NoSuchElementException();
        }



    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here

        return fieldSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<Type> typeList=new ArrayList<>();
        List<String> nameList=new ArrayList<>();

        for(TDItem item:td1.listOfTDItem){
            typeList.add(item.fieldType);
            nameList.add(item.fieldName);
        }

        for(TDItem item:td2.listOfTDItem){
            typeList.add(item.fieldType);
            nameList.add(item.fieldName);
        }

        return new TupleDesc(typeList.toArray(new Type[typeList.size()]),nameList.toArray(new String[nameList.size()]));//注意这个转array的方法
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {//可以参考下别人的写法
        // some code goes here
        if(this==o)return true;
        if(!(o instanceof TupleDesc))return false;
        TupleDesc outTupleDesc=(TupleDesc)o;
        if(outTupleDesc.fieldLength!=this.fieldLength)
        return false;
        for(int i=0;i<this.fieldLength;i++){
            if(!(this.getFieldType(i)==outTupleDesc.getFieldType(i))||!(this.getFieldName(i).equals(outTupleDesc.getFieldName(i))))return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb=new StringBuilder();
        for(TDItem item:this.listOfTDItem){
            sb.append(item.fieldType.toString()+item.fieldName.toString()+",");
        }
        return sb.deleteCharAt(sb.length()-1).toString();
    }
}
