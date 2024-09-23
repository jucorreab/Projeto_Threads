package Trabalho1;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class TemperaturaCidades {

    private static final String CAMINHO_ARQUIVOS = "//Users//juliacorrea//eclipse-workspace//Projeto//src//temperaturas_cidades";
    private static final int NUMERO_REPETICOES = 10;
    private static final int NUMERO_CIDADES = 320;
    private static final int ANOS_INICIO = 1995;
    private static final int ANOS_FIM = 2020;

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
            long tempoInicio = System.currentTimeMillis();
            resultadosGlobais.clear();

            int numThreads = definirNumeroThreads(experimento);
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            if (experimento <= 10) {
                int cidadesPorThread = NUMERO_CIDADES / numThreads;
                for (int i = 0; i < numThreads; i++) {
                    int cidadeInicial = i * cidadesPorThread;
                    int cidadeFinal = (i == numThreads - 1) ? NUMERO_CIDADES : cidadeInicial + cidadesPorThread;
                    executor.execute(new ProcessadorCidades(experimento, cidadeInicial, cidadeFinal));
                }
            } else {
                int numCidadesThreads = Math.min(NUMERO_CIDADES, numThreads);
                int cidadesPorThread = NUMERO_CIDADES / numCidadesThreads;
                for (int i = 0; i < numCidadesThreads; i++) {
                    int cidadeInicial = i * cidadesPorThread;
                    int cidadeFinal = (i == numCidadesThreads - 1) ? NUMERO_CIDADES : cidadeInicial + cidadesPorThread;
                    executor.execute(new ProcessadorCidadesComThreadsPorAno(experimento, cidadeInicial, cidadeFinal));
                }
            }

            awaitTerminationAfterShutdown(executor);

            long tempoFim = System.currentTimeMillis();
            temposRodadas[rodada] = tempoFim - tempoInicio;
            System.out.println("Experimento " + experimento + " - Rodada " + (rodada + 1) + ": " + (tempoFim - tempoInicio) + " ms");
        }

        calcularEMostraTempoMedio(experimento, temposRodadas);
    }

    private static int definirNumeroThreads(int experimento) {
        if (experimento == 1) {
            return 1;
        } else if (experimento <= 10) {
            return (int) Math.pow(2, experimento - 1);
        } else {
            return NUMERO_CIDADES;
        }
    }

    private static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
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
        long somaTempos = Arrays.stream(temposRodadas).sum();
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
                br.readLine();

                Map<Integer, DadosTemperaturaMes> dadosPorMes = new ConcurrentHashMap<>();
                String linha;
                while ((linha = br.readLine()) != null) {
                    String[] campos = linha.split(",");
                    int mes = Integer.parseInt(campos[2]);
                    int ano = Integer.parseInt(campos[4]);
                    double temperatura = Double.parseDouble(campos[5]);

                    dadosPorMes.computeIfAbsent(mes, k -> new DadosTemperaturaMes()).adicionarTemperatura(ano, temperatura);
                }

                salvarResultadosPorAno(nomeCidade, dadosPorMes);

            } catch (IOException e) {
                System.err.println("Erro ao ler arquivo: " + e.getMessage());
            }
        }

        private void salvarResultadosPorAno(String nomeCidade, Map<Integer, DadosTemperaturaMes> dadosPorMes) {
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

    static class ProcessadorCidadesComThreadsPorAno implements Runnable {
        private final int experimento;
        private final int cidadeInicial;
        private final int cidadeFinal;

        public ProcessadorCidadesComThreadsPorAno(int experimento, int cidadeInicial, int cidadeFinal) {
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
                br.readLine();

                Map<Integer, DadosTemperaturaMes> dadosPorMes = new ConcurrentHashMap<>();
                String linha;
                while ((linha = br.readLine()) != null) {
                    String[] campos = linha.split(",");
                    int mes = Integer.parseInt(campos[2]);
                    int ano = Integer.parseInt(campos[4]);
                    double temperatura = Double.parseDouble(campos[5]);

                    dadosPorMes.computeIfAbsent(mes, k -> new DadosTemperaturaMes()).adicionarTemperatura(ano, temperatura);
                }

                ExecutorService executorAnos = Executors.newFixedThreadPool(ANOS_FIM - ANOS_INICIO + 1);
                for (int ano = ANOS_INICIO; ano <= ANOS_FIM; ano++) {
                    final int anoAtual = ano;
                    executorAnos.execute(() -> salvarResultadosPorAno(nomeCidade, dadosPorMes, anoAtual));
                }

                awaitTerminationAfterShutdown(executorAnos);

            } catch (IOException e) {
                System.err.println("Erro ao ler arquivo: " + e.getMessage());
            }
        }

        private void salvarResultadosPorAno(String nomeCidade, Map<Integer, DadosTemperaturaMes> dadosPorMes, int ano) {
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

    static class DadosTemperaturaMes {
        private final Map<Integer, List<Double>> temperaturasPorAno = new HashMap<>();

        public void adicionarTemperatura(int ano, double temperatura) {
            temperaturasPorAno.computeIfAbsent(ano, k -> new ArrayList<>()).add(temperatura);
        }

        public double getTemperaturaMedia(int ano) {
            return temperaturasPorAno.getOrDefault(ano, Collections.emptyList()).stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
        }

        public double getTemperaturaMaxima(int ano) {
            return temperaturasPorAno.getOrDefault(ano, Collections.emptyList()).stream().mapToDouble(Double::doubleValue).max().orElse(Double.NaN);
        }

        public double getTemperaturaMinima(int ano) {
            return temperaturasPorAno.getOrDefault(ano, Collections.emptyList()).stream().mapToDouble(Double::doubleValue).min().orElse(Double.NaN);
        }
    }

    private static void salvarResultados() {
        try (FileWriter writer = new FileWriter("resultados_finais.txt")) {
            for (String cidade : resultadosGlobais.keySet()) {
                writer.write("Cidade: " + cidade + "\n");
                Map<Integer, Map<Integer, double[]>> anosMeses = resultadosGlobais.get(cidade);
                for (int ano : anosMeses.keySet()) {
                    writer.write("  Ano: " + ano + "\n");
                    for (int mes : anosMeses.get(ano).keySet()) {
                        double[] temperaturas = anosMeses.get(ano).get(mes);
                        writer.write(String.format("    Mês %d: Média: %.2f, Máxima: %.2f, Mínima: %.2f\n", mes, temperaturas[0], temperaturas[1], temperaturas[2]));
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Erro ao salvar resultados finais: " + e.getMessage());
        }
    }
}
