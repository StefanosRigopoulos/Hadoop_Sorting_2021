import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper <LongWritable, Text, IntWritable, Text> {
	
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

	public int sumSpent, sumIncome, count, spent = 0, incomeValue = 0;
	private IntWritable ID = new IntWritable();
	private String subYear, income, mntWines, mntFruits, mntMeatProducts, mntFishProducts, mntSweetProducts, mntGoldProducts;
	
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
		
		File file = new File("/home/hdoop/cheats4.txt");
		BufferedWriter bw = new BufferedWriter(new FileWriter(file));

		String line = value.toString().replace("\t", "");
		String[] arrOfVal = line.split(",");
		
		// Setting the income value.
		income = arrOfVal[4];
		
		// Setting up the subscription year
		String[] subDate = arrOfVal[7].split("/");
		subYear = subDate[2];
		
		// Setting spent values.
		mntWines = arrOfVal[9];
		mntFruits = arrOfVal[10];
		mntMeatProducts = arrOfVal[11];
		mntFishProducts = arrOfVal[12];
		mntSweetProducts = arrOfVal[13];
		mntGoldProducts = arrOfVal[14];
		
		// Making the summary.
		try {
			spent = Integer.parseInt(mntWines) + Integer.parseInt(mntFruits) + Integer.parseInt(mntMeatProducts) + 
				    Integer.parseInt(mntFishProducts) + Integer.parseInt(mntSweetProducts) + Integer.parseInt(mntGoldProducts);
			incomeValue = Integer.parseInt(income);
		} catch (NumberFormatException e) {
			spent = 0;
		}
		sumSpent += spent;
		sumIncome += incomeValue;
		count++;
		
		// Writing our cheats.
		bw.write(sumSpent + "," + sumIncome + "," + count);
		bw.close();
		
		String info = "," + income + "," + subYear + "," + spent;
		
		// Setting ID.
		try {
			ID.set(Integer.parseInt(arrOfVal[0]));
			context.write(ID, new Text(info));
		} catch (NumberFormatException e) {
			// do nothing
		}
	}
}
