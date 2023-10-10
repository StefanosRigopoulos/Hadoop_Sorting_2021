import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortWineMapper extends Mapper <LongWritable, Text, IntWritable, Text> {
	
	private Text info = new Text();
	
	/* 00:ID,
	 * 01:Year_Birth,
	 * 02:Education,
	 * 03:Marital_Status,
	 * 04:Income,
	 * 05:Kidhome,
	 * 06:Teenhome,
	 * 07:Dt_Customer,
	 * 08:Recency,
	 * 09:MntWines,
	 * 10:MntFruits,
	 * 11:MntMeatProducts,
	 * 12:MntFishProducts,
	 * 13:MntSweetProducts,
	 * 14:MntGoldProds,
	 * 15:NumDealsPurchases,
	 * 16:NumWebPurchases,
	 * 17:NumCatalogPurchases,
	 * 18:NumStorePurchases,
	 * 19:NumWebVisitsMonth,
	 * 20:AcceptedCmp3,
	 * 21:AcceptedCmp4,
	 * 22:AcceptedCmp5,
	 * 23:AcceptedCmp1,
	 * 24:AcceptedCmp2,
	 * 25:Complain,
	 * 26:Response,,,,
	 */
	
	/* -1 ==== ranking
	 * 00 ==== ID
	 * 01 ==== Age (Year_Birth)
	 * 02 ==== Education
	 * 03 ==== Marital_Status
	 * 04 ==== Income
	 * 09 ==== MntWines
	 */

	private String id = "", education = "", marital_status = "", income = "", mntWines = "", infoString = "";
	private int age;
	public int sum, count, spent = 0;
	
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

		File file = new File("/home/hdoop/cheats3.txt");
		BufferedWriter bw = new BufferedWriter(new FileWriter(file));
		
		String line = value.toString().replace("\t", "");
		String[] arrOfStr = line.split(",");
		
		// setting variables
		id = arrOfStr[0];
		age = 2021 - Integer.parseInt(arrOfStr[1]);
		education = arrOfStr[2];
		marital_status = arrOfStr[3];
		income = arrOfStr[4];
		mntWines = arrOfStr[9];
		
		infoString =  "," + id + "," + age + "," + education + "," + marital_status + "," + income + "," + mntWines;
		info.set(infoString);

		// Making the summary.
		try {
			spent = Integer.parseInt(mntWines);
		} catch (NumberFormatException e) {
			spent = 0;
		}
		sum += spent;
		count++;
		
		// Writing our cheats.
		bw.write(sum + "," + count);
		bw.close();
		
		context.write(new IntWritable(Integer.parseInt(mntWines)), new Text(info));
	}
}