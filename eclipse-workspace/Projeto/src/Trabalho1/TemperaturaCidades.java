package Trabalho1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

//testando se sobe no git

public class TemperaturaCidades {

    private static final String CAMINHO_ARQUIVOS = "//Users//juliacorrea//eclipse-workspace//Projeto//src//temperaturas_cidades";
    private static final int NUMERO_REPETICOES = 10;
    private static final int NUMERO_CIDADES = 320;
    private static final int ANOS_INICIO = 1995;
    private static final int ANOS_FIM = 2020;

    // Mapa global para armazenar os resultados finais (Cidade -> Ano -> Mês -> Temperaturas)
    private static Map<String, Map<Integer, Map<Integer, double[]>>> resultadosGlobais = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        for (int experimento = 1; experimento <= 20; experimento++) {
            executarExperimento(experimento);
        }

        salvarResultados();
    }

    private static void executarExperimento(int experimento) {
        long[] temposRodadas = new long[NUMERO_REPETICOES];

        for (int rodada = 0; rodada < NUMERO_REPETICOES; rodada++) {
            long tempoInicio = System.currentTimeMillis(); // Tempo inicial
            resultadosGlobais.clear();

            int numThreads = definirNumeroThreads(experimento);
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            int cidadesPorThread = NUMERO_CIDADES / numThreads;
            for (int i = 0; i < numThreads; i++) {
                int cidadeInicial = i * cidadesPorThread;
                int cidadeFinal = (i == numThreads - 1) ? NUMERO_CIDADES : cidadeInicial + cidadesPorThread;
                executor.execute(new ProcessadorCidades(experimento, cidadeInicial, cidadeFinal));
            }

            awaitTerminationAfterShutdown(executor); // Aguarda a conclusão das threads

            long tempoFim = System.currentTimeMillis(); // Tempo final
            temposRodadas[rodada] = tempoFim - tempoInicio; // Cálculo do tempo de execução
            System.out.println("Experimento " + experimento + " - Rodada " + (rodada + 1) + ": "
                    + (tempoFim - tempoInicio) + " ms");
        }

        calcularEMostraTempoMedio(experimento, temposRodadas);
    }


 // Método para definir o número de threads
    private static int definirNumeroThreads(int experimento) {
        if (experimento == 1) {
            return 1; // Primeira versão sem threads
        } else if (experimento <= 10) {
            return (int) Math.pow(2, experimento - 1); // Primeiras 10 versões variando o número de threads por cidade
        } else {
            return ANOS_FIM - ANOS_INICIO + 1; // Segundas 10 versões, threads para cada ano processado
        }
    }

    // Método para aguardar o término das threads
    private static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Timeout while waiting for thread pool termination!");
            }
        } catch (InterruptedException ex) {
            System.err.println("Interrupted while waiting for thread pool termination!");
            Thread.currentThread().interrupt();
        }
    }

    // Método para calcular e mostrar o tempo médio
    private static void calcularEMostraTempoMedio(int experimento, long[] temposRodadas) {
        long somaTempos = 0;
        for (long tempo : temposRodadas) {
            somaTempos += tempo;
        }
        long tempoMedio = somaTempos / NUMERO_REPETICOES;

        try (FileWriter writer = new FileWriter("versao_" + experimento + ".txt")) {
            for (int i = 0; i < NUMERO_REPETICOES; i++) {
                writer.write("Rodada " + (i + 1) + ": " + temposRodadas[i] + " ms\n");
            }
            writer.write("Tempo Médio: " + tempoMedio + " ms\n");
        } catch (IOException e) {
            System.err.println("Erro ao salvar os tempos em arquivo: " + e.getMessage());
        }
    }

    static class ProcessadorCidades implements Runnable {
        private int experimento;
        private int cidadeInicial;
        private int cidadeFinal;

        public ProcessadorCidades(int experimento, int cidadeInicial, int cidadeFinal) {
            this.experimento = experimento;
            this.cidadeInicial = cidadeInicial;
            this.cidadeFinal = cidadeFinal;
        }

        @Override
        public void run() {
            for (int i = cidadeInicial; i < cidadeFinal; i++) {
                String nomeArquivo = obterNomeArquivoCidade(i);
                processarCidade(nomeArquivo);
            }
        }

        private String obterNomeArquivoCidade(int indiceCidade) {
            try (Stream<Path> paths = Files.list(Paths.get(CAMINHO_ARQUIVOS))) {
                return paths
                        .skip(indiceCidade)
                        .findFirst()
                        .orElseThrow(() -> new IOException("Arquivo não encontrado para o índice " + indiceCidade))
                        .getFileName()
                        .toString();
            } catch (IOException e) {
                System.err.println("Erro ao listar arquivos: " + e.getMessage());
                return "";
            }
        }

        private void processarCidade(String nomeArquivo) {
            String caminhoCompleto = String.format("%s/%s", CAMINHO_ARQUIVOS, nomeArquivo);
            String nomeCidade = nomeArquivo.replace(".csv", "").replace("__", " - ");

            try (BufferedReader br = new BufferedReader(new FileReader(caminhoCompleto))) {
                br.readLine(); // Pula o cabeçalho

                Map<Integer, DadosTemperaturaMes> dadosPorMes = new ConcurrentHashMap<>();
                String linha;
                while ((linha = br.readLine()) != null) {
                    String[] campos = linha.split(",");
                    int mes = Integer.parseInt(campos[2]);
                    int ano = Integer.parseInt(campos[4]);
                    double temperatura = Double.parseDouble(campos[5]);

                    dadosPorMes.computeIfAbsent(mes, k -> new DadosTemperaturaMes())
                            .adicionarTemperatura(ano, temperatura);
                }

                // Calcula as temperaturas para cada mês de cada ano
                for (int ano = ANOS_INICIO; ano <= ANOS_FIM; ano++) {
                    Map<Integer, double[]> resultadosPorMes = new HashMap<>();
                    for (Map.Entry<Integer, DadosTemperaturaMes> entry : dadosPorMes.entrySet()) {
                        int mes = entry.getKey();
                        DadosTemperaturaMes dadosMes = entry.getValue();

                        double temperaturaMedia = dadosMes.getTemperaturaMedia(ano);
                        double temperaturaMaxima = dadosMes.getTemperaturaMaxima(ano);
                        double temperaturaMinima = dadosMes.getTemperaturaMinima(ano);

                        resultadosPorMes.put(mes,
                                new double[] { temperaturaMedia, temperaturaMaxima, temperaturaMinima });
                    }

                    // Armazena os resultados no mapa global
                    resultadosGlobais.computeIfAbsent(nomeCidade, k -> new ConcurrentHashMap<>()).put(ano,
                            resultadosPorMes);
                }

                if (experimento > 10) {
                    ExecutorService executorAnos = Executors.newFixedThreadPool(ANOS_FIM - ANOS_INICIO + 1);
                    for (int ano = ANOS_INICIO; ano <= ANOS_FIM; ano++) {
                        executorAnos.execute(new ProcessadorAno(nomeCidade, dadosPorMes, ano));
                    }
                    TemperaturaCidades.awaitTerminationAfterShutdown(executorAnos);
                } else {
                    // Para experimentos 1 a 10, processa todos os anos em uma única thread
                    for (int ano = ANOS_INICIO; ano <= ANOS_FIM; ano++) {
                        new ProcessadorAno(nomeCidade, dadosPorMes, ano).run();
                    }
                }

            } catch (IOException e) {
                System.err.println("Erro ao ler arquivo: " + e.getMessage());
            }
        }
    }

    static class ProcessadorAno implements Runnable {
        private String nomeCidade;
        private Map<Integer, DadosTemperaturaMes> dadosPorMes;
        private int ano;

        public ProcessadorAno(String nomeCidade, Map<Integer, DadosTemperaturaMes> dadosPorMes, int ano) {
            this.nomeCidade = nomeCidade;
            this.dadosPorMes = dadosPorMes;
            this.ano = ano;
        }

        @Override
        public void run() {
            // Este método não é mais necessário, pois o cálculo é feito na classe ProcessadorCidades
        }
    }

    static class DadosTemperaturaMes {
        private Map<Integer, List<Double>> temperaturasPorAno = new HashMap<>();

        public void adicionarTemperatura(int ano, double temperatura) {
            temperaturasPorAno.computeIfAbsent(ano, k -> new ArrayList<>()).add(temperatura);
        }

        public double getTemperaturaMedia(int ano) {
            List<Double> temperaturas = temperaturasPorAno.getOrDefault(ano, new ArrayList<>());
            if (temperaturas.isEmpty()) {
                return Double.NaN;
            }
            double soma = 0;
            for (Double temp : temperaturas) {
                soma += temp;
            }
            return soma / temperaturas.size();
        }

        public double getTemperaturaMaxima(int ano) {
            return temperaturasPorAno.getOrDefault(ano, new ArrayList<>()).stream()
                    .mapToDouble(Double::doubleValue)
                    .max()
                    .orElse(Double.NaN);
        }

        public double getTemperaturaMinima(int ano) {
            return temperaturasPorAno.getOrDefault(ano, new ArrayList<>()).stream()
                    .mapToDouble(Double::doubleValue)
                    .min()
                    .orElse(Double.NaN);
        }
    }

    private static void salvarResultados() {
        for (Map.Entry<String, Map<Integer, Map<Integer, double[]>>> cidadeEntry : resultadosGlobais.entrySet()) {
            String nomeCidade = cidadeEntry.getKey();
            Map<Integer, Map<Integer, double[]>> anos = cidadeEntry.getValue();

            System.out.println("Cidade: " + nomeCidade);
            for (int ano = ANOS_INICIO; ano <= ANOS_FIM; ano++) {
                System.out.println("Ano: " + ano);

                Map<Integer, double[]> meses = anos.get(ano);

                if (meses != null) {
                    for (Map.Entry<Integer, double[]> mesEntry : meses.entrySet()) {
                        int mes = mesEntry.getKey();
                        double[] temperaturas = mesEntry.getValue();

                        // Tratamento de NaN e formatação
                        String media = Double.isNaN(temperaturas[0]) ? "N/D" : String.format("%.2f", temperaturas[0]).replace('.', ',');
                        String maxima = Double.isNaN(temperaturas[1]) ? "N/D" : String.format("%.2f", temperaturas[1]).replace('.', ',');
                        String minima = Double.isNaN(temperaturas[2]) ? "N/D" : String.format("%.2f", temperaturas[2]).replace('.', ',');

                        // Saída formatada
                        System.out.printf("  Mês: %d - Média: %s, Máxima: %s, Mínima: %s\n", mes, media, maxima, minima);
                    }
                } else {
                    System.out.println("  Sem dados para este ano.");
                }
                System.out.println(); // Adiciona uma linha em branco após cada ano
            }
        }
    }

}