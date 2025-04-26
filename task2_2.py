from mrjob.job import MRJob, MRStep
import pandas as pd

top_users = pd.read_csv("task2_2_users_total_spending_output.txt", sep= "\t", header=None, names=["User", "Spending"], dtype={0: str})["User"].to_list()

class spendingOfTop5 (MRJob):
    def steps(self):
        return [MRStep(mapper= self.mapper_1, reducer= self.reducer_1),
                MRStep(mapper= self.mapper_2, reducer= self.reducer_2)]
    
    def mapper_1(self, _, line):
        transaction = line.split("\t")
        yield transaction[1]+": " +transaction[5]+ "-" +transaction[4] ,float(transaction[2])

    def reducer_1(self, user, sale):
        yield user, round(sum(sale), 2)

    def mapper_2(self, user, sales):
        if user.split(": ")[0] in top_users:
            yield None, (user, sales)
    
    def reducer_2(self, _, sales):
        sales_list = list(sales)
        sorted_sales = sorted(sales_list, key=lambda p: p[0])

        for user, sale in sorted_sales:
            yield user, sale


if __name__ == "__main__":
    spendingOfTop5.run()    