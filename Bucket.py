'''
Nesta biblioteca esta implementado a estrutura do bucket, temos um bucket mais simples e suas operações especificas
e uma estrutua de bucket principal, que alem do bucket simples, precisa de uma forma para armazenar os buckets de overflow que são
criados quando um  bucket enche. Foi utilizado a herança no bucket principal, no qual ele herda as estruturas e operações do bucket simples.

Quando um bucket de overflow é criado no bucket simples, tem se uma lista que é utlizada para armazenar buckets simples. Ao inserir um bucket
na lista de buckets de overflow, não é preciso que este bucket seja do mesmo tipo que o bucket principal, mas simplesmente um bucket comun e capaz de
armazenar as entradas de dados e realizar as operações sobre estas entradas.
'''



class Bucket:
    def __init__(self, capacidadeMaxima):
        self.capacidadeMaxima = capacidadeMaxima
        self.bucket = list() #Lista que armazena os registros


    def verificaSeBucketEstaCheio(self):
        return (len(self.bucket) == self.capacidadeMaxima)

    def verificaSeBucketEstaVazio(self):
        return (len(self.bucket) == 0)

    def insereEntrada(self,entrada):
        self.bucket.append(entrada)

    def quantEntradasNoBucket(self): # Verifica quantas entradas(Registros) estão no bucket
        return len(self.bucket)

    def buscaEntradaNoBucket(self,chave): #Busca uma entrada(Registro) no bucket
        quantEntradasNoBucket = self.quantEntradasNoBucket()

        for k in range(quantEntradasNoBucket):
            if (self.bucket[k][0] == chave):
                return self.bucket[k]
        return 0 # Retorna 0 se não encontrar

    def removerEntradaNoBucket(self,chave): #Remove um registro no bucket
        quantEntradasNoBucket = self.quantEntradasNoBucket()

        for k in range(quantEntradasNoBucket):

            if (self.bucket[k][0] == chave):

                self.bucket.remove(self.bucket[k])
                return 1
        return 0 #Retorna 0 se não conseguir remover

class conteinerBuckets(Bucket):

    def __init__(self, capacidadeMaxima):
        Bucket.__init__(self,capacidadeMaxima)
        self.bucketOverflow = list() #lista que armazena os buckets simples após o bucket principal encher. Tais buckets são overFlows que recebem os
                                    # registros.

    def verificaSeHaBucketsOverFlow(self):#Verifica se há buckets de overflow, necessário na redistribuição dos registros
        return (len(self.bucketOverflow) > 0)


    def quantBucketOverFlow(self):
        return (len(self.bucketOverflow))