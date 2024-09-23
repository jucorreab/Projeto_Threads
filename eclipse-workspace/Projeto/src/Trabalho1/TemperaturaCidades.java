package Trabalho1;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class TemperaturaCidades {

    // Caminho para os arquivos CSV 
    private static final String CAMINHO_ARQUIVOS = "//Users//juliacorrea//eclipse-workspace//Projeto//src//temperaturas_cidades";
    private static final int NUMERO_REPETICOES = 10;
    private static final int NUMERO_CIDADES = 320;
    private static final int ANOS_INICIO = 1995;
    private static final int ANOS_FIM = 2020;

    // Mapa global para armazenar resultados de todas as cidades e anos
    private static Map<String, Map<Integer, Map<Integer, double[]>>> resultadosGlobais = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        // Executa 20 experimentos diferentes
        for (int experimento = 1; experimento <= 20; experimento++) {
            executarExperimento(experimento);
        }
        // Salva todos os resultados após a execução dos experimentos
        salvarResultados();
    }

    private static void executarExperimento(int experimento) {
        long[] temposRodadas = new long[NUMERO_REPETICOES];

        // Executa cada rodada do experimento
        for (int rodada = 0; rodada < NUMERO_REPETICOES; rodada++) {
            long tempoInicio = System.currentTimeMillis(); // Marca o tempo de início
            resultadosGlobais.clear(); // Limpa os resultados anteriores

            // Define o número de threads a serem usadas
            int numThreads = definirNumeroThreads(experimento);
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            
            
            // Processa as cidades com base no experimento
            if (experimento <= 10) {
                int cidadesPorThread = NUMERO_CIDADES / numThreads;
                for (int i = 0; i < numThreads; i++) {
                    int cidadeInicial = i * cidadesPorThread;
                    int cidadeFinal = (i == numThreads - 1) ? NUMERO_CIDADES : cidadeInicial + cidadesPorThread;
                    executor.execute(new ProcessadorCidades(experimento, cidadeInicial, cidadeFinal));
                }
            } else {
                // Para as versões de 11 a 20, cada cidade é processada em uma thread separada
                for (int i = 0; i < NUMERO_CIDADES; i++) {
                    executor.execute(new ProcessadorCidadesComThreadsPorAno(experimento, i));
                }
            }

            awaitTerminationAfterShutdown(executor); // Aguarda o término das threads

            long tempoFim = System.currentTimeMillis(); // Marca o tempo de fim
            temposRodadas[rodada] = tempoFim - tempoInicio; // Calcula o tempo da rodada
            System.out.println("Experimento " + experimento + " - Rodada " + (rodada + 1) + ": "
                    + (tempoFim - tempoInicio) + " ms");
        }

        // Calcula e exibe o tempo médio de execução
        calcularEMostraTempoMedio(experimento, temposRodadas);
    }

    private static int definirNumeroThreads(int experimento) {
        // Define o número de threads com base no experimento
        if (experimento == 1) {
            return 1; // Uma thread para o primeiro experimento
        } else if (experimento <= 10) {
            return (int) Math.pow(2, experimento - 1); // Dobra o número de threads a cada experimento
        } else {
            return NUMERO_CIDADES; // Uma thread por cidade para os experimentos de 11 a 20
        }
    }

    private static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown(); // Desliga o pool de threads
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Timeout ao aguardar o término das threads!");
            }
        } catch (InterruptedException ex) {
            System.err.println("Interrompido ao aguardar o término das threads!");
            Thread.currentThread().interrupt();
        }
    }

    private static void calcularEMostraTempoMedio(int experimento, long[] temposRodadas) {
        // Calcula o tempo médio das rodadas
        long somaTempos = Arrays.stream(temposRodadas).sum();
        long tempoMedio = somaTempos / NUMERO_REPETICOES;

        // Salva os tempos de execução em um arquivo
        try (FileWriter writer = new FileWriter("versao_" + experimento + ".txt")) {
            for (int i = 0; i < NUMERO_REPETICOES; i++) {
                writer.write("Rodada " + (i + 1) + ": " + temposRodadas[i] + " ms\n");
            }
            writer.write("Tempo Médio: " + tempoMedio + " ms\n");
        } catch (IOException e) {
            System.err.println("Erro ao salvar os tempos em arquivo: " + e.getMessage());
        }
    }

    // Classe para processar cidades em threads
    static class ProcessadorCidades implements Runnable {
        private final int experimento;
        private final int cidadeInicial;
        private final int cidadeFinal;

        public ProcessadorCidades(int experimento, int cidadeInicial, int cidadeFinal) {
            this.experimento = experimento;
            this.cidadeInicial = cidadeInicial;
            this.cidadeFinal = cidadeFinal;
        }

        @Override
        public void run() {
            // Processa cada cidade no intervalo definido
            for (int i = cidadeInicial; i < cidadeFinal; i++) {
                String nomeArquivo = obterNomeArquivoCidade(i);
                processarCidade(nomeArquivo);
            }
        }

        private String obterNomeArquivoCidade(int indiceCidade) {
            // Obtém o nome do arquivo da cidade com base no índice
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
            // Processa os dados de temperatura da cidade a partir do arquivo CSV
            String caminhoCompleto = String.format("%s/%s", CAMINHO_ARQUIVOS, nomeArquivo);
            String nomeCidade = nomeArquivo.replace(".csv", "").replace("__", " - ");

            try (BufferedReader br = new BufferedReader(new FileReader(caminhoCompleto))) {
                br.readLine(); // Ignora a primeira linha (cabeçalho)

                Map<Integer, DadosTemperaturaMes> dadosPorMes = new ConcurrentHashMap<>();
                String linha;
                while ((linha = br.readLine()) != null) {
                    String[] campos = linha.split(",");
                    int mes = Integer.parseInt(campos[2]);
                    int ano = Integer.parseInt(campos[4]);
                    double temperatura = Double.parseDouble(campos[5]);

                    // Armazena as temperaturas por mês
                    dadosPorMes.computeIfAbsent(mes, k -> new DadosTemperaturaMes()).adicionarTemperatura(ano, temperatura);
                }

                salvarResultadosPorAno(nomeCidade, dadosPorMes); // Salva os resultados por ano

            } catch (IOException e) {
                System.err.println("Erro ao ler arquivo: " + e.getMessage());
            }
        }

        private void salvarResultadosPorAno(String nomeCidade, Map<Integer, DadosTemperaturaMes> dadosPorMes) {
            // Salva os resultados das temperaturas para cada mês e ano
            for (int ano = ANOS_INICIO; ano <= ANOS_FIM; ano++) {
                for (int mes = 1; mes <= 12; mes++) {
                    double temperaturaMedia = dadosPorMes.getOrDefault(mes, new DadosTemperaturaMes()).getTemperaturaMedia(ano);
                    double temperaturaMaxima = dadosPorMes.getOrDefault(mes, new DadosTemperaturaMes()).getTemperaturaMaxima(ano);
                    double temperaturaMinima = dadosPorMes.getOrDefault(mes, new DadosTemperaturaMes()).getTemperaturaMinima(ano);

                    resultadosGlobais.computeIfAbsent(nomeCidade, k -> new ConcurrentHashMap<>())
                            .computeIfAbsent(ano, k -> new HashMap<>())
                            .put(mes, new double[]{temperaturaMedia, temperaturaMaxima, temperaturaMinima});
                }
            }
        }
    }

    // Classe para processar cidades com uma thread por ano
    static class ProcessadorCidadesComThreadsPorAno implements Runnable {
        private final int experimento;
        private final int indiceCidade;

        public ProcessadorCidadesComThreadsPorAno(int experimento, int indiceCidade) {
            this.experimento = experimento;
            this.indiceCidade = indiceCidade;
        }

        @Override
        public void run() {
            // Processa a cidade e cria threads para salvar resultados por ano
            String nomeArquivo = obterNomeArquivoCidade(indiceCidade);
            processarCidade(nomeArquivo);
        }

        private String obterNomeArquivoCidade(int indiceCidade) {
            // Obtém o nome do arquivo da cidade com base no índice
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
            // Processa os dados de temperatura da cidade a partir do arquivo CSV
            String caminhoCompleto = String.format("%s/%s", CAMINHO_ARQUIVOS, nomeArquivo);
            String nomeCidade = nomeArquivo.replace(".csv", "").replace("__", " - ");

            try (BufferedReader br = new BufferedReader(new FileReader(caminhoCompleto))) {
                br.readLine(); // Ignora a primeira linha (cabeçalho)

                Map<Integer, DadosTemperaturaMes> dadosPorMes = new ConcurrentHashMap<>();
                String linha;
                while ((linha = br.readLine()) != null) {
                    String[] campos = linha.split(",");
                    int mes = Integer.parseInt(campos[2]);
                    int ano = Integer.parseInt(campos[4]);
                    double temperatura = Double.parseDouble(campos[5]);

                    // Armazena as temperaturas por mês
                    dadosPorMes.computeIfAbsent(mes, k -> new DadosTemperaturaMes()).adicionarTemperatura(ano, temperatura);
                }

                // Criar uma thread para cada ano e processar
                for (int ano = ANOS_INICIO; ano <= ANOS_FIM; ano++) {
                    new Thread(() -> salvarResultadosPorAno(nomeCidade, dadosPorMes)).start();
                }

            } catch (IOException e) {
                System.err.println("Erro ao ler arquivo: " + e.getMessage());
            }
        }

        private void salvarResultadosPorAno(String nomeCidade, Map<Integer, DadosTemperaturaMes> dadosPorMes) {
            // Salva os resultados das temperaturas para cada mês e ano
            for (int ano = ANOS_INICIO; ano <= ANOS_FIM; ano++) {
                for (int mes = 1; mes <= 12; mes++) {
                    double temperaturaMedia = dadosPorMes.getOrDefault(mes, new DadosTemperaturaMes()).getTemperaturaMedia(ano);
                    double temperaturaMaxima = dadosPorMes.getOrDefault(mes, new DadosTemperaturaMes()).getTemperaturaMaxima(ano);
                    double temperaturaMinima = dadosPorMes.getOrDefault(mes, new DadosTemperaturaMes()).getTemperaturaMinima(ano);

                    resultadosGlobais.computeIfAbsent(nomeCidade, k -> new ConcurrentHashMap<>())
                            .computeIfAbsent(ano, k -> new HashMap<>())
                            .put(mes, new double[]{temperaturaMedia, temperaturaMaxima, temperaturaMinima});
                }
            }
        }
    }

    // Armazena dados de temperatura por mês
    static class DadosTemperaturaMes {
        private Map<Integer, List<Double>> temperaturasPorAno = new ConcurrentHashMap<>();

        public void adicionarTemperatura(int ano, double temperatura) {
            // Adiciona a temperatura à lista do ano correspondente
            temperaturasPorAno.computeIfAbsent(ano, k -> new ArrayList<>()).add(temperatura);
        }

        public double getTemperaturaMedia(int ano) {
            List<Double> temperaturas = temperaturasPorAno.get(ano);
            if (temperaturas == null || temperaturas.isEmpty()) {
                return Double.NaN; // Retorna NaN se não houver dados
            }
            return temperaturas.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
        }

        public double getTemperaturaMaxima(int ano) {
            List<Double> temperaturas = temperaturasPorAno.get(ano);
            if (temperaturas == null || temperaturas.isEmpty()) {
                return Double.NaN; // Retorna NaN se não houver dados
            }
            return temperaturas.stream().mapToDouble(Double::doubleValue).max().orElse(Double.NaN);
        }

        public double getTemperaturaMinima(int ano) {
            List<Double> temperaturas = temperaturasPorAno.get(ano);
            if (temperaturas == null || temperaturas.isEmpty()) {
                return Double.NaN; // Retorna NaN se não houver dados
            }
            return temperaturas.stream().mapToDouble(Double::doubleValue).min().orElse(Double.NaN);
        }
    }

    private static void salvarResultados() {
        // Salva e exibe os resultados finais de todas as cidades
        for (Map.Entry<String, Map<Integer, Map<Integer, double[]>>> cidadeEntry : resultadosGlobais.entrySet()) {
            String cidade = cidadeEntry.getKey();
            System.out.println("Cidade: " + cidade);
            for (Map.Entry<Integer, Map<Integer, double[]>> anoEntry : cidadeEntry.getValue().entrySet()) {
                int ano = anoEntry.getKey();
                for (Map.Entry<Integer, double[]> mesEntry : anoEntry.getValue().entrySet()) {
                    int mes = mesEntry.getKey();
                    double[] dados = mesEntry.getValue();
                    System.out.printf("Ano: %d, Mês: %d, Média: %.2f, Máxima: %.2f, Mínima: %.2f%n",
                            ano, mes, dados[0], dados[1], dados[2]);
                }
            }
        }
    }
}