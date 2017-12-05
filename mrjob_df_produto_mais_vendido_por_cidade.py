from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd

class MRProdutoMaisVendidoPorCidade(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]
        
    def mapper(self, _, line):
        #2015-01-01;09:00:00;Sao Paulo;Roupas masculinas;214.05;Amex

        (data, hora, cidade, produto, valor, formaPagto) = line.split(';')

        #chave = cidade + '-' + produto
            
        yield cidade, (produto, 1)
    
    def reducer(self, cidade, produtos):   
        df = pd.DataFrame(produtos)
        grp = df.groupby(by=0)
        psum = grp.sum().reset_index()
        psorted = psum.sort_values(1, ascending=False)
        qty =  int(list(psorted[:1][1])[0])
        prd = str(list(psorted[:1][0])[0])

        yield cidade, (prd, qty)
        
           


if __name__ == '__main__':
    MRProdutoMaisVendidoPorCidade.run()