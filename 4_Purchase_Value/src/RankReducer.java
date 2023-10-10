import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
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
	
    public void reduce(Text values, IntWritable key, Context context) throws IOException, InterruptedException {	
        
		context.write(values, key);
    }
}
