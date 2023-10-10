import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FindEbMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
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
	
	/* Graduation
	 * PhD
	 * Master
	 * Basic
	 * 2n Cycle
	 */

	private String graduation = "Graduation";
	private String phd = "PhD";
	private String master = "Master";
	private String basic = "Basic";
	private String cycle = "2n Cycle";
	
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
		
		String line = value.toString().replace("\"", "");
		String[] arrOfStr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		int counter = 0;
		
		for(String a : arrOfStr) {
			if(counter == 2) {

				// Type of education.
				word.set(a);
				
				// Checking the graduation type.
				if(a.compareTo(graduation) == 0) {
					word.set(graduation);
					context.write(word, one);
				} else if(a.compareTo(phd) == 0) {
					word.set(phd);
					context.write(word, one);
				} else if(a.compareTo(master) == 0) {
					word.set(master);
					context.write(word, one);
				} else if(a.compareTo(basic) == 0) {
					word.set(basic);
					context.write(word, one);
				} else if(a.compareTo(cycle) == 0) {
					word.set(cycle);
					context.write(word, one);
				} else {
					word.set("wrong");
					context.write(word, one);
				}
			}
			counter++;
		}
	}
}
