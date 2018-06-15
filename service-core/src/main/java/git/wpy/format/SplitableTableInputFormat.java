package git.wpy.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class SplitableTableInputFormat extends TableInputFormat {
    //TODO 最大1.7G
    private static long maxSize = (long) (0.02 * 1000 * 1000 * 1000L);

    @Override
    public List<InputSplit> calculateRebalancedSplits(List<InputSplit> list, JobContext context,
                                                      long average) {
        List<InputSplit> resultList = new ArrayList<InputSplit>();
        Configuration conf = context.getConfiguration();
        TableName tableName = TableName.valueOf(conf.get(INPUT_TABLE));
        //The default data skew ratio is 3
//        float dataSkewRatio = conf.getFloat(INPUT_AUTOBALANCE_MAXSKEWRATIO, 0.6f);
        //It determines which mode to use: text key mode or binary key mode. The default is text mode.
        boolean isTextKey = context.getConfiguration().getBoolean(TABLE_ROW_TEXTKEY, true);
        long dataSkewThreshold = maxSize;
        int count = 0;
        while (count < list.size()) {
            TableSplit ts = (TableSplit) list.get(count);
            String regionLocation = ts.getRegionLocation();
            long regionSize = ts.getLength();
            if (regionSize >= dataSkewThreshold) {
                // if the current region size is large than the data skew threshold,
                // split the region into two MapReduce input splits.
                List<byte[]> splits = new ArrayList<>();
                splits.add(ts.getStartRow());
                split(ts.getStartRow(), ts.getEndRow(), regionSize, splits, isTextKey);
                splits.add(ts.getEndRow());
                int len = splits.size();
                long averageSize = regionSize / len;
                long totalSize = 0;
                for (int i = 0; i < len - 2; i++) {
                    TableSplit t = new TableSplit(tableName, splits.get(i), splits.get(i + 1), regionLocation,
                            averageSize);
                    resultList.add(t);
                    totalSize += averageSize;
                }
                TableSplit t = new TableSplit(tableName, splits.get(len - 2), splits.get(len - 1), regionLocation,
                        regionSize - totalSize);
                resultList.add(t);
                count++;
            } else if (regionSize >= average) {
                // if the region size between average size and data skew threshold size,
                // make this region as one MapReduce input split.
                resultList.add(ts);
                count++;
            } else {
                // if the total size of several small continuous regions less than the average region size,
                // combine them into one MapReduce input split.
                long totalSize = regionSize;
                byte[] splitStartKey = ts.getStartRow();
                byte[] splitEndKey = ts.getEndRow();
                count++;
                for (; count < list.size(); count++) {
                    TableSplit nextRegion = (TableSplit) list.get(count);
                    long nextRegionSize = nextRegion.getLength();
                    if (totalSize + nextRegionSize <= dataSkewThreshold) {
                        totalSize = totalSize + nextRegionSize;
                        splitEndKey = nextRegion.getEndRow();
                    } else {
                        break;
                    }
                }
                TableSplit t = new TableSplit(tableName, splitStartKey, splitEndKey,
                        regionLocation, totalSize);
                resultList.add(t);
            }
        }
        return resultList;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        return super.getSplits(context);
    }

    @Override
    public RecordReader<ImmutableBytesWritable, Result> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
//        KerberosUtil.config(getConf());
        return super.createRecordReader(split, context);
    }

    public void split(byte[] start, byte[] stop, long size, List<byte[]> list, boolean isTextKey) {
        if (size > maxSize) {
            byte[] left = getSplitKeyV2(start, stop, isTextKey);
            size /= 2;
            split(start, left, size, list, isTextKey);
            list.add(left);
            split(left, stop, size, list, isTextKey);
        }
    }

    public byte[] getSplitKeyV2(byte[] start, byte[] end, boolean isText) {
        byte upperLimitByte;
        byte lowerLimitByte;
        //Use text mode or binary mode.
        if (isText) {
            //The range of text char set in ASCII is [32,126], the lower limit is space and the upper
            // limit is '~'.
            upperLimitByte = '~';
            lowerLimitByte = ' ';
        } else {
            upperLimitByte = Byte.MAX_VALUE;
            lowerLimitByte = Byte.MIN_VALUE;
        }
        // For special case
        // Example 1 : startkey=null, endkey="hhhqqqwww", splitKey="h"
        // Example 2 (text key mode): startKey="ffffaaa", endKey=null, splitkey="f~~~~~~"
        if (start.length == 0 && end.length == 0) {
            return new byte[]{(byte) ((lowerLimitByte + upperLimitByte) / 2)};
        }
        if (start.length == 0 && end.length != 0) {
            return new byte[]{end[0]};
        }
        if (start.length != 0 && end.length == 0) {
            byte[] result = new byte[start.length];
            result[0] = start[0];
            for (int k = 1; k < start.length; k++) {
                result[k] = upperLimitByte;
            }
            return result;
        }
        // A list to store bytes in split key
        List resultBytesList = new ArrayList();
        int maxLength = start.length > end.length ? start.length : end.length;
        for (int i = 0; i < maxLength; i++) {
            //calculate the midpoint byte between the first difference
            //for example: "11ae" and "11chw", the midpoint is "11b"
            //another example: "11ae" and "11bhw", the first different byte is 'a' and 'b',
            // there is no midpoint between 'a' and 'b', so we need to check the next byte.
            if (start[i] == end[i]) {
                resultBytesList.add(start[i]);
                //For special case like: startKey="aaa", endKey="aaaz", splitKey="aaaM"
                if (i + 1 == start.length) {
                    if (end.length > start.length)
                        resultBytesList.add((byte) ((lowerLimitByte + end[i + 1]) / 2));
                    break;
                }
            } else {
                //if the two bytes differ by 1, like ['a','b'], We need to check the next byte to find
                // the midpoint.
                if ((int) end[i] - (int) start[i] == 1) {
                    //get next byte after the first difference
                    byte startNextByte = (i + 1 < start.length) ? start[i + 1] : lowerLimitByte;
                    byte endNextByte = (i + 1 < end.length) ? end[i + 1] : lowerLimitByte;
                    int byteRange = (upperLimitByte - startNextByte) + (endNextByte - lowerLimitByte) + 1;
                    int halfRange = byteRange / 2;
                    if ((int) startNextByte + halfRange > (int) upperLimitByte) {
                        resultBytesList.add(end[i]);
                        resultBytesList.add((byte) (startNextByte + halfRange - upperLimitByte +
                                lowerLimitByte));
                    } else {
                        resultBytesList.add(start[i]);
                        resultBytesList.add((byte) (startNextByte + halfRange));
                    }
                } else {
                    //calculate the midpoint key by the fist different byte (normal case),
                    // like "11ae" and "11chw", the midpoint is "11b"
                    resultBytesList.add((byte) ((start[i] + end[i]) / 2));
                }
                break;
            }
        }
        //transform the List of bytes to byte[]
        byte result[] = new byte[resultBytesList.size()];
        for (int k = 0; k < resultBytesList.size(); k++) {
            result[k] = (byte) resultBytesList.get(k);
        }
        return result;
    }
}
