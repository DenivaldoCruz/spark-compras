from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter

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
            
        yield cidade, produto
        

    def reducer(self, cidade, produtos):   

        lista_produtos = {}
        for p in produtos:
            if p not in lista_produtos:
                lista_produtos[p] = 1
            else:
                lista_produtos[p] = lista_produtos[p] + 1

        count = {}    
        for key in lista_produtos:
            count[lista_produtos[key]] =  key

        yield cidade, (count[max(count)], max(count))
           


if __name__ == '__main__':
    MRProdutoMaisVendidoPorCidade.run()