from mrjob.job import MRJob, MRStep

class TopCoffeeConsumers(MRJob):

    def steps(self):
        return [MRStep(mapper= self.mapper_1, reducer= self.reducer_1),
                MRStep(mapper= self.mapper_2, reducer= self.reducer_2)]
    
    def mapper_1 (self, _, line):
        transaction = line.split("\t")
        yield transaction[1] , float(transaction[2])

    def reducer_1 (self, user, sale):
        yield user, round(sum(sale), 2)

    def mapper_2 (self, user, sales):
        yield None , (user, sales)

    def reducer_2 (self, _, sales):
        all_sales = list(sales)
        sorted_all_sales = sorted(all_sales, key=lambda p: p[1], reverse=True)

        for user, sale in sorted_all_sales[:5]:
            yield user, sale

if __name__ == "__main__":
    TopCoffeeConsumers.run()