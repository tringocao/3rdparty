
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.lang.Math;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.io.File;
import java.util.Scanner;
import java.io.FileNotFoundException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map.Entry;

/**
 *
 * @author minhquang
 */
public class Native3 {

    public static void main(String[] args) throws Exception {

        HashMap<String, Double> list = new HashMap<>();

        try {
            File file = new File(args[0] + "/res/re.txt");
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                String[] res = sc.nextLine().split("\t", 2);
                String[] res1 = res[0].split("&", 4);
                Double SIM = (Double.parseDouble(res[1])
                        / (Double.parseDouble(res1[1]) + Double.parseDouble(res1[3])
                        - Double.parseDouble(res[1])));
                if (checkMap(list, res1[0], SIM)) {
                    list.put(res1[0], SIM);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }

        Map<String, Double> sortedMapAsc = sortByComparator(list, false);
        printMap(sortedMapAsc);

    }

    private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap, final boolean order) {

        List<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Double>>() {
            public int compare(Entry<String, Double> o1,
                    Entry<String, Double> o2) {
                if (order) {
                    return o1.getValue().compareTo(o2.getValue());
                } else {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
        for (Entry<String, Double> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    public static void printMap(Map<String, Double> map) {
        int i = 0;
        for (Entry<String, Double> entry : map.entrySet()) {
            System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
            i++;
            if (i == 10) {
                break;
            }

        }
    }

    public static boolean checkMap(Map<String, Double> map, String text, Double x) {
        for (Entry<String, Double> entry : map.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(text)) {
                if (entry.getValue().compareTo(x) > 0.0) {
                    return false;
                }
            }
        }
        return true;

    }
}

