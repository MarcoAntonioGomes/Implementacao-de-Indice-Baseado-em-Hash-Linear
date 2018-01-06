from Bucket import *
'''
Biblioteca Principal 
Nesta biblioteca implementou se as principais funções e objetos para que o Hashing Linear funcione.
A class que define o objeto Hashing Linear é responsavel por receber os parâmetros principais do hashing e dos registros que irão ser armazenados e
também por implementar as operações principais, inserção, remoção e busca por igualdade para o hashing linear. 
'''


class hashingLinear:
    def __init__(self, qtdCampos, nivelInicial, tamanhoDoBlocoDePaginas,tamanhoDoNumeroInteiro):
        self.tamanhoDoNumeroInteiro = tamanhoDoNumeroInteiro
        self.tamanhoDoBlocoDePaginas = tamanhoDoBlocoDePaginas
        self.qtdCampos = qtdCampos # Quantidade de campos do registro
        self.numeroDeBucketsInicial = (2**nivelInicial) #Numero de Buckets inicial baseado no nível inicial do hashing
        self.numeroDeBucketsAposNivelAumentar = self.numeroDeBucketsInicial
        self.nivel = 0
        self.proximo = 0
        self.capacidadeDoBucket = int(self.tamanhoDoBlocoDePaginas / (self.qtdCampos * self.tamanhoDoNumeroInteiro))
        self.listaDeBuckets = list()
        self.alocaBuckets()


    def alocaBuckets(self): # Aloca os buckets na lista de buckets quando o objeto é criado;
        for i in range(self. numeroDeBucketsInicial):
            self.listaDeBuckets.append(conteinerBuckets(self.capacidadeDoBucket))

    '''
    Função de hashing - retorna qual indice de bucket para a inserção baseado em uma chave para que possa 
    ser realizado a inserção de um registro no arquivo.
    '''

    def funcaoHNivel(self, H, nivel):

        if(nivel == 0):
            return (H % (self.numeroDeBucketsInicial))
        else:
            return (H % (self.numeroDeBucketsAposNivelAumentar))


    '''
    Procedimento que insere os registros nos buckets apropriados 
    '''

    def insereEntradaDeDadosNoBucket(self,entrada):

        chave = entrada[0]
        bucketAInserir = self.funcaoHNivel(chave,self.nivel) # Chama a função de hashing para obtenção de um indice do bucket de inserção.
       # print("Inserindo Entrada(Registro) \n Registro: ",entrada)

        if not self.listaDeBuckets[bucketAInserir].verificaSeBucketEstaCheio(): # Se não esta cheio posso inserir normalmente
            self.listaDeBuckets[bucketAInserir].bucket.append(entrada)
            if(self.listaDeBuckets[bucketAInserir].verificaSeBucketEstaCheio()): # Se encheu preciso inserir um bucket  na lista de buckets de
                                                                                 # OverFlow para receber o proximo registro que venha a cair neste
                                                                                 # bucket
                self.listaDeBuckets[bucketAInserir].bucketOverflow.append(Bucket(self.capacidadeDoBucket))
        else:
            quantidadeDeBucketsOverFlow = self.listaDeBuckets[bucketAInserir].quantBucketOverFlow() # Bucket Cheio - insiro nos buckets de Overflow
            for i in range(quantidadeDeBucketsOverFlow): #Laço para percorrer ate que se acha um bucket  de overFlow vazio.
                if(not self.listaDeBuckets[bucketAInserir].bucketOverflow[i].verificaSeBucketEstaCheio()):# Se estiver vazio posso inserir

                    self.listaDeBuckets[bucketAInserir].bucketOverflow[i].insereEntrada(entrada)
                    if(self.listaDeBuckets[bucketAInserir].bucketOverflow[i].quantEntradasNoBucket() == 1):
                        self.dividirBucket() #Inserir no bucket de overFlow causa uma divisão no bucket que o proximo aponta.
                    break
                else: #Bucket de overflow que vou inserir ta cheio e não tem mais buckets de overflow vazio, cria se um novo bucket na lista para
                      # receber o registro
                    self.listaDeBuckets[bucketAInserir].bucketOverflow.append(Bucket(self.capacidadeDoBucket))
                    self.listaDeBuckets[bucketAInserir].bucketOverflow[i+1].insereEntrada(entrada)
                    break

    '''
    Função para retornar quantos buckets eu tenho baseado em um nível 
    Usada para resetar o proximo e incrementar o nivel, quando proximo é igual Nnivel - 1 (Dividi todos os buckets do Nível)
    '''

    def retornaNnivel(self):
        return self.numeroDeBucketsInicial*(2**self.nivel)

    '''
    Procedimento para redistribuir as entradas no processo de divisão.
    '''

    def redistribuirEntradas(self,indice,entrada):

        bucketAinserir = self.listaDeBuckets[indice] #Pego novo bucket a inserir
        if not bucketAinserir.verificaSeBucketEstaCheio(): #Se ele não estiver cheio, insiro nele
            bucketAinserir.bucket.append(entrada)

        elif (bucketAinserir.verificaSeHaBucketsOverFlow()): # Estando cheio insiro nos buckets de overFlow apropriados.
            quantidadeDeBucketsOverFlow = bucketAinserir.quantBucketOverFlow()
            for i in range(quantidadeDeBucketsOverFlow):
                if not bucketAinserir.bucketOverflow[i].verificaSeBucketEstaCheio():
                    bucketAinserir.bucketOverflow[i].insereEntrada(entrada)
                    break
        else:
            bucketAinserir.bucketOverflow.append(Bucket(self.capacidadeDoBucket))
            bucketAinserir.bucketOverflow[0].insereEntrada(entrada)

    '''
    Procedimento responsável por controlar a divisão dos buckets no qual o proximo aponta.
    
    '''


    def dividirBucket(self):
        entradasAremover = list() # Lista Usada para remover os registros que sairam do bucket e foram para outro bucket.

        self.listaDeBuckets.append(conteinerBuckets(self.capacidadeDoBucket)) # Insere um novo bucket, para que possa receber possiveis registros
                                                                              # redistribuidos.
        qtdEntradasNoBucket = self.listaDeBuckets[self.proximo].quantEntradasNoBucket() #Pega quantos registros terão que ser redistribuídos

        for i in range((qtdEntradasNoBucket)):
            entradaARemover = self.listaDeBuckets[self.proximo].bucket[i] #Receber o registro a ser redistribuido
            chave = entradaARemover[0]
            novoBucketAinserir = self.funcaoHNivel(chave, self.nivel + 1) #Aplica Função de hashing usando nivel +1 para saber o novo indice de
                                                                          # bucket a ser removido.
            if(novoBucketAinserir != self.proximo): #Se e novo bucket for diferente do atual posso inserir, senão o registro continua no bucket
                entradasAremover.append(entradaARemover) #Armazena o registro que esta sendo removido.
                self.redistribuirEntradas(novoBucketAinserir,entradaARemover) # Coloca o registro no novo bucket imagem do bucket na posiçao proximo

        for i in range(len(entradasAremover)): #Remove os registros que sairam do bucket[proximo] e foram para o bucket[Imagem]
            self.listaDeBuckets[self.proximo].bucket.remove(entradasAremover[i])
        entradasAremover = list()

        if(self.listaDeBuckets[self.proximo].verificaSeHaBucketsOverFlow()): # Redistribui os registros presentes nos buckets de overFlow
                                                                             # Um bucket[proximo] pode ter buckets de overflow com registros
            quantBucketsOverFlow = self.listaDeBuckets[self.proximo].quantBucketOverFlow()

            for i in range(quantBucketsOverFlow):

                quantEntradasBucket =  self.listaDeBuckets[self.proximo].bucketOverflow[i].quantEntradasNoBucket()
                for j in range(quantEntradasBucket):
                    bucketComEntradas =  self.listaDeBuckets[self.proximo].bucketOverflow[i].bucket
                    entradaARemover = self.listaDeBuckets[self.proximo].bucketOverflow[i].bucket[j]
                    chave = entradaARemover[0]
                    novoBucketAinserir = self.funcaoHNivel(chave, self.nivel + 1)

                    if (novoBucketAinserir != self.proximo):
                        entradasAremover.append(entradaARemover)
                        self.redistribuirEntradas(novoBucketAinserir, entradaARemover)

                for k in range(len(entradasAremover)):
                    bucketComEntradas.remove(entradasAremover[k])
                entradasAremover = list()


        if (self.proximo == (self.retornaNnivel()-1)): #Se o proximo for iguual Nnivel-1 significa que todos os buckets foram divididos.
            self.nivel +=1 #Nivel aumenta +1
            self.numeroDeBucketsAposNivelAumentar = self.numeroDeBucketsAposNivelAumentar*2 #numero de buckets duplica
            self.proximo = -1 # linha 151 e 153 garatem que proximo zere

        self.proximo += 1 #Incrementa o proximo para que assim um novo bucket seja dividido quando uma novo de bucket de overflow com registro for
                          #inserido.

    '''
    Procedimento responsável por controlar a busca por igualdade de um registro nos buckets 
    '''

    def realizaBuscaPorIgualdade(self, chave):
        bucketDeBusca = self.funcaoHNivel(chave, self.nivel) #Recebe o Bucket candidato a ter o registro


        if bucketDeBusca > self.proximo: # Se o bucketDeBusca for maior que o proximo, quer dizer que ele não foi dividido, logo, o registro
                                         # se encontra nele
            entrada = self.buscaPorIgualdade(bucketDeBusca,chave) #Chama a função que busca o registro, passando o bucketDebusca e a chave
            if(entrada == None):
                print("Erro!!! Entrada com chave, [",chave,"], não encontrada")
            else:
                print("Dados encontrados...\n Entrada de Dados: ", entrada)
        else:
            entrada = self.buscaPorIgualdade(bucketDeBusca,chave) # Se o bucketDeBusca não for maior que o proximo, o registro pode estar nele
                                                                  # ou no seu bucket imagem dividida. Primeiro verifica - se se esta no bucketDeBusca
            if(entrada == None):
                bucketDeBusca = self.funcaoHNivel(chave,(self.nivel+1)) # Não esta, aplica a função de hashing com nivel + 1 e obtem se o bucket
                                                                        # de imagem.

            entrada = self.buscaPorIgualdade(bucketDeBusca,chave) #Realiza a busca no bucket imagem.
            if (entrada == None):
                print("Erro!!! Entrada com chave, [", chave, "], não encontrada")
            else:
                print("Dados encontrados...\n Entrada de Dados: ", entrada)

    '''
    Como nos buckets principais podemos ter buckets de overFlow com registros, foi criada uma função para realizar a busca dos registros
    trabalhando também com a busca em cima dos buckets de overflow. 
    
    '''

    def buscaPorIgualdade(self,bucketDeBusca,chave):

        entrada = self.listaDeBuckets[bucketDeBusca].buscaEntradaNoBucket(chave) #Verifica se o registro esta no bucket Principal
        if(type(entrada) == list):
            return entrada
        else: # Senão estiver no bucket principal, realiza se a busca nos buckets de overflow
            bucketsOverFlow = self.listaDeBuckets[bucketDeBusca].bucketOverflow
            quanTBucketsOverFlow = self.listaDeBuckets[bucketDeBusca].quantBucketOverFlow()
            for i in range (quanTBucketsOverFlow):
                entrada =  bucketsOverFlow[i].buscaEntradaNoBucket(chave)
                if(type(entrada) == list):
                    return entrada
            return None #Não encontrou nos buckets de overflow.

    '''
    Procedimento encarregado de fazer a pré remoção de registro nos buckets, este procedimento assim como no da busca,
    controla qual bucket o registro estará. (0 até Proximo ou Proximo até Nnivel)   
          
    '''

    def descobreBucketERemoveEntrada(self, chave):
        bucketDeBusca = self.funcaoHNivel(chave, self.nivel)

        if bucketDeBusca > self.proximo: #Se estiver entre proximo e Nnivel tenta remover no bucketDebusca no qual usou se a funaoHnivel(Chave,nivel)
            removeu = self.removeEntrada(bucketDeBusca,chave) #Igual na busca, chama outro metodo que tenta remover no bucket principal ou no overflow
            #if(removeu == 1):
            #print("Entrada removida com sucesso!!!")
            #else:
                #print("Erro!!! Entrada com chave, [", chave, "], não encontrada")
        else: # Bucket de busca menor que o proximo, ou seja entre 0 e proximo, registro a ser removido pode estar no bucket de Busca ou na sua imagem
              # dividida, os procedimentos da linha 220 a 231 funcionam parecido com a busca para este caso, tentando remover o registro no bucket
              # principal ou na sua imagem.
            removeu = self.removeEntrada(bucketDeBusca,chave)
            if (removeu == 1):
                #print("Entrada removida com sucesso!!!")
                return
            else:
                bucketDeBusca = self.funcaoHNivel(chave,(self.nivel+1))

            removeu = self.removeEntrada(bucketDeBusca,chave)
            #if (removeu == 1):
                #print("Entrada removida com sucesso!!!")
            #else:
                #print("Erro!!! Entrada com chave, [", chave, "], não encontrada")

    '''
    Função semelhante a função de busca que ajuda a remover a entrada(Registro) tratando para o caso de se ter registros em buckets de overflow.
    
    '''

    def removeEntrada(self,bucketDeBusca,chave):


        removeu = self.listaDeBuckets[bucketDeBusca].removerEntradaNoBucket(chave)

        if(removeu == 1):

            return removeu
        else:
            bucketsOverFlow = self.listaDeBuckets[bucketDeBusca].bucketOverflow
            quanTBucketsOverFlow = self.listaDeBuckets[bucketDeBusca].quantBucketOverFlow()



            for i in range (quanTBucketsOverFlow):
                removeu =  bucketsOverFlow[i].removerEntradaNoBucket(chave)

                if(removeu == 1):
                    return removeu
            return 0