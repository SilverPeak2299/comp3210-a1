from mrjob.job import MRJob, MRStep
import pandas as pd

total_sales = pd.read_csv("task1_2_output.txt", sep= "\t", header=None, names=["Coffee", "Sales"])
smallest_sales = total_sales.nsmallest(3, 'Sales')["Coffee"].tolist()



class LeastSoldCoffee(MRJob):

    def steps(self):
        return [MRStep(mapper= self.mapper_1, reducer= self.reducer_1),
                MRStep(mapper= self.mapper_2, reducer= self.reducer_2)]
    
    def mapper_1(self, _, line):
        transaction = line.split("\t")
        yield transaction[0]+": " +transaction[5]+ "-" +transaction[4] ,float(transaction[1])

    def reducer_1(self, coffee_type, cost):
        yield coffee_type, sum(cost)

    def mapper_2(self, coffee_sales,summed_cost):
        yield None, (coffee_sales, summed_cost)

    def reducer_2(self, _, coffee_info):
        pairs = list(coffee_info)
        sorted_pairs = sorted(pairs, key=lambda p: p[0], reverse=False)
        
        for word, count in sorted_pairs:
            if word.split(": ")[0] in smallest_sales:
                yield word, count    

if __name__ == "__main__":
    LeastSoldCoffee.run()




