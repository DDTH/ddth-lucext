package com.github.ddth.lucext.directory;

import java.util.Map;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.lucext.utils.IdUtils;

/**
 * File metadata info.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class FileInfo implements Cloneable {
    public final static FileInfo[] EMPTY_ARRAY = new FileInfo[0];

    public static FileInfo newInstance() {
        String id = IdUtils.nextId();
        FileInfo fileInfo = new FileInfo();
        fileInfo.setId(id).setSize(0);
        return fileInfo;
    }

    public static FileInfo newInstance(String name) {
        FileInfo fileInfo = newInstance();
        fileInfo.setName(name);
        return fileInfo;
    }

    @SuppressWarnings("unchecked")
    public static FileInfo newInstance(byte[] data) {
        if (data == null || data.length <= 8) {
            return null;
        }
        Map<String, Object> dataMap = SerializationUtils.fromByteArray(data, Map.class);
        if (dataMap == null) {
            return null;
        }
        FileInfo fileInfo = newInstance();
        fileInfo.fromMap(dataMap);
        return fileInfo;
    }

    /*----------------------------------------------------------------------*/

    public FileInfo clone() {
        try {
            FileInfo clone = (FileInfo) super.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    private String id, name;
    private long size;

    public String getId() {
        return id;
    }

    public FileInfo setId(String value) {
        this.id = value != null ? value.trim().toLowerCase() : null;
        return this;
    }

    public String getName() {
        return name;
    }

    public FileInfo setName(String value) {
        this.name = value != null ? value.trim() : null;
        return this;
    }

    public long getSize() {
        return size;
    }

    public FileInfo setSize(long value) {
        this.size = value;
        return this;
    }

    /**
     * Export this file metadata info as a map.
     * 
     * @return
     */
    public Map<String, Object> asMap() {
        return MapUtils.createMap("id", id, "name", name, "size", size);
    }

    /**
     * Import file metadata info from a map (created via {@link #asMap()}.
     * 
     * @param data
     * @return
     */
    public FileInfo fromMap(Map<String, Object> data) {
        setId(MapUtils.getValue(data, "id", String.class));
        setName(MapUtils.getValue(data, "name", String.class));
        Long size = MapUtils.getValue(data, "size", Long.class);
        setSize(size != null ? size.longValue() : 0);
        return this;
    }

    /**
     * Export this file metadata info as a byte array.
     * 
     * @return
     */
    public byte[] asBytes() {
        Map<String, Object> data = asMap();
        return data != null ? SerializationUtils.toByteArray(data) : null;
    }
}
