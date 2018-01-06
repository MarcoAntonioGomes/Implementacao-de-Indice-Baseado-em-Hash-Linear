from  estruturaDoBucket import*
import time

if __name__ == "__main__":

    inicio = time.time()

    arq = open('default.csv','r')

    nivelInicial = 5
    tamanhoDoBloco = 8000
    tamanhoDoInteiro = 28
    quanTRegistrosInseridos = 0

    for linha in arq:
        valores = linha.split(',')
        if (valores[0] == '+'):
            quantidadeDeCampos = (len(valores) - 1)
            break

    novoHashing = hashingLinear(quantidadeDeCampos,nivelInicial,tamanhoDoBloco,tamanhoDoInteiro)
    entrada = list()
    chavesEntradaAremover = list()

    #print("Inserindo no Hashing")


    for linha in arq:
        valores = linha.split(',')
        if(valores[0] == '+'):
            entrada = list(map(int, valores[1:]))
            novoHashing.insereEntradaDeDadosNoBucket(entrada) #Insere os registros
            quanTRegistrosInseridos += 1
        elif(valores[0] == '-'):
            entrada = list(map(int, valores[1:]))
            chavesEntradaAremover.append(entrada[0])
    for l in range(len(chavesEntradaAremover)):
        chave = chavesEntradaAremover[l]
        novoHashing.descobreBucketERemoveEntrada(chave) #Remove os registros

    arq.close()
    # print("\n--------------------------------------------------------------------------------------------------------------------------------------------\n")
    # print("\nBuscando Entrada ja removida...\n")
    # novoHashing.realizaBuscaPorIgualdade(3451) #Busca entrada já remnovida
    # print("\nTentando remover entrada ja removida ...\n")
    # novoHashing.descobreBucketERemoveEntrada(6949) #Tenta remover entrada já removida
    # print("\nBuscando entrada não removida ...\n")
    # novoHashing.realizaBuscaPorIgualdade(4135) #Busca entrada não removida

    fim = time.time()
    print("\n\nTempo de Execução: ", fim - inicio)