package BigT;

import java.util.Arrays;
import java.util.HashSet;

public class GetCounts {

    HashSet<String> allBigt;
    int numBuf;

    public GetCounts(HashSet<String> allBigt, int numBuf) {
        this.allBigt = allBigt;
        this.numBuf = numBuf;
    }

    public void run() throws Exception {
        int rowCount;
        int colCount;
        bigt tempBigt;
        int mapCount;

        for (String bigtName : allBigt) {
            System.out.println("Bigtable: " + bigtName);
            System.out.println("---------------------------------------");
            tempBigt = new bigt(bigtName, true);
            mapCount = tempBigt.getMapCnt();
            rowCount = tempBigt.getRowCnt();
            colCount = tempBigt.getColumnCnt();
            System.out.println("TOTAL NUMBER OF MAPS: " + mapCount);
            System.out.println("NUMBER OF DISTINCT ROW LABELS: " + rowCount);
            System.out.println("NUMBER OF DISTINCT COLUMN LABELS: " + colCount);
            System.out.println("---------------------------------------");
            tempBigt.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: java GetCounts <NUMBUF> <bigtable1,bigtable2,...>");
            System.exit(1);
        }

        int numBuf = Integer.parseInt(args[0]);
        String[] bigtableNames = args[1].split(",");
        HashSet<String> allBigt = new HashSet<>(Arrays.asList(bigtableNames));

        GetCounts getAllCount = new GetCounts(allBigt, numBuf);
        getAllCount.run();
    }
}
