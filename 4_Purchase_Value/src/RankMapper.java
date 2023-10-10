import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
	
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
	
	/*
	 * Gold : dt_Customer_Year == 21
	 *      | income > 69500
	 *      | MntWines + MntFruits + MntMeatProducts + MntFishProducts + MntSweetProducts + MntGoldProducts > average*3/2
	 * 
	 * Silver : dt_Customer_Year < 21
	 *        | income > 69500
	 *        | MntWines, MntFruits, MntMeatProducts, MntFishProducts, MntSweetProducts, MntGoldProducts > average*3/2
	 * 
	 * Bronze : dt_Customer_Year == 21
	 *        | income < average
	 *        | MntWines, MntFruits, MntMeatProducts, MntFishProducts, MntSweetProducts, MntGoldProducts > average*1/4
	 * 
	 * Paper : dt_Customer_Year < 21
	 *       | income < average
	 *       | MntWines, MntFruits, MntMeatProducts, MntFishProducts, MntSweetProducts, MntGoldProducts > average*1/4
	 */
	
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
		
		File file = new File("/home/hdoop/cheats4.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		
		String textInfo = value.toString();
		textInfo = textInfo.replaceAll("\t", "");
		String[] text = textInfo.split(",");
		
		int id = 0, income = 0, year = 0, spent = 0;
		
		try  {
			id = Integer.parseInt(text[0]);
			income = Integer.parseInt(text[1]);
			year = Integer.parseInt(text[2]);
			spent = Integer.parseInt(text[3]);
		} catch (NumberFormatException e) {
			income = 0;
			year = 0;
			spent = 0;
		}
		
		// Getting the average
		String line = br.readLine();
		br.close();
		String[] arr = line.split(",");
		int sumSpent = 0, sumIncome = 0, count = 0;
		try {
			sumSpent = Integer.parseInt(arr[0]);
			sumIncome = Integer.parseInt(arr[1]);
			count = Integer.parseInt(arr[2]);
		} catch (NumberFormatException e) {
			// do nothing
		}
		double averageIncome = sumIncome / count;
		double averageSpent = sumSpent / count;
		
		// Final separation between GOLD and SILVER.
		if (income > 69500 && spent >= (averageSpent*3)/2) {
			if (year == 21) { 				// Gold user.
				context.write(new Text("GOLD"), new IntWritable(id));
			} else if (year < 21) { 		// Silver user.
				context.write(new Text("SILVER"), new IntWritable(id));
			}
		} else if (income < averageIncome && spent <= (averageSpent*1)/4) {
			if (year == 21) { 				// Bronze user.
				context.write(new Text("BRONZE"), new IntWritable(id));
			} else if (year < 21) { 		// Paper user.
				context.write(new Text("PAPER"), new IntWritable(id));
			}
		}
		//context.write(new Text(income + ";" + year + ";" + spent + ";"), new IntWritable(id));
    }
}
