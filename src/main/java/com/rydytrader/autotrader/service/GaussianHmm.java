package com.rydytrader.autotrader.service;

import java.util.Random;

/**
 * Gaussian Hidden Markov Model with diagonal covariance.
 *
 * Observations: T x D matrix (T time steps, D features)
 * States: N hidden states (typically 3)
 *
 * Algorithms:
 * - Baum-Welch EM (fit)
 * - Viterbi (most likely state sequence)
 * - Forward (posterior probabilities)
 */
public class GaussianHmm {

    private final int numStates;
    private final int numFeatures;
    private final Random random;

    // Model parameters
    private double[] pi;            // initial state probabilities [N]
    private double[][] trans;       // transition matrix [N][N]
    private double[][] means;       // emission means [N][D]
    private double[][] variances;   // emission variances (diagonal cov) [N][D]

    public GaussianHmm(int numStates, int numFeatures) {
        this(numStates, numFeatures, 42L);
    }

    public GaussianHmm(int numStates, int numFeatures, long seed) {
        this.numStates = numStates;
        this.numFeatures = numFeatures;
        this.random = new Random(seed);
    }

    public int getNumStates() { return numStates; }
    public double[][] getMeans() { return means; }
    public double[][] getVariances() { return variances; }
    public double[][] getTransitionMatrix() { return trans; }

    /**
     * Fit the HMM to observations using Baum-Welch EM.
     * observations: T x D matrix
     * iterations: number of EM iterations (typically 20-50)
     */
    public void fit(double[][] observations, int iterations) {
        int T = observations.length;
        if (T < numStates) throw new IllegalArgumentException("Not enough observations");

        initializeParameters(observations);

        for (int iter = 0; iter < iterations; iter++) {
            // E-step: forward-backward
            double[][] alpha = forward(observations);
            double[][] beta = backward(observations);

            // Compute posteriors (gamma) and transition posteriors (xi)
            double[][] gamma = new double[T][numStates];
            for (int t = 0; t < T; t++) {
                double sum = 0;
                for (int i = 0; i < numStates; i++) {
                    gamma[t][i] = alpha[t][i] * beta[t][i];
                    sum += gamma[t][i];
                }
                if (sum > 0) {
                    for (int i = 0; i < numStates; i++) gamma[t][i] /= sum;
                }
            }

            double[][][] xi = new double[T - 1][numStates][numStates];
            for (int t = 0; t < T - 1; t++) {
                double sum = 0;
                for (int i = 0; i < numStates; i++) {
                    for (int j = 0; j < numStates; j++) {
                        xi[t][i][j] = alpha[t][i] * trans[i][j] * emissionProb(observations[t + 1], j) * beta[t + 1][j];
                        sum += xi[t][i][j];
                    }
                }
                if (sum > 0) {
                    for (int i = 0; i < numStates; i++) {
                        for (int j = 0; j < numStates; j++) xi[t][i][j] /= sum;
                    }
                }
            }

            // M-step: update parameters
            // Update pi
            for (int i = 0; i < numStates; i++) pi[i] = gamma[0][i];

            // Update transition matrix
            for (int i = 0; i < numStates; i++) {
                double denom = 0;
                for (int t = 0; t < T - 1; t++) denom += gamma[t][i];
                for (int j = 0; j < numStates; j++) {
                    double num = 0;
                    for (int t = 0; t < T - 1; t++) num += xi[t][i][j];
                    trans[i][j] = denom > 0 ? num / denom : 1.0 / numStates;
                }
            }

            // Update means and variances
            for (int i = 0; i < numStates; i++) {
                double denom = 0;
                for (int t = 0; t < T; t++) denom += gamma[t][i];
                if (denom <= 0) continue;

                for (int d = 0; d < numFeatures; d++) {
                    double meanNum = 0;
                    for (int t = 0; t < T; t++) meanNum += gamma[t][i] * observations[t][d];
                    means[i][d] = meanNum / denom;
                }
                for (int d = 0; d < numFeatures; d++) {
                    double varNum = 0;
                    for (int t = 0; t < T; t++) {
                        double diff = observations[t][d] - means[i][d];
                        varNum += gamma[t][i] * diff * diff;
                    }
                    variances[i][d] = Math.max(varNum / denom, 1e-6); // floor for numerical stability
                }
            }
        }
    }

    /** Forward algorithm: returns alpha[T][N]. */
    public double[][] forward(double[][] observations) {
        int T = observations.length;
        double[][] alpha = new double[T][numStates];

        // Initialize
        double sum = 0;
        for (int i = 0; i < numStates; i++) {
            alpha[0][i] = pi[i] * emissionProb(observations[0], i);
            sum += alpha[0][i];
        }
        // Normalize to avoid underflow
        if (sum > 0) for (int i = 0; i < numStates; i++) alpha[0][i] /= sum;

        // Recursion
        for (int t = 1; t < T; t++) {
            sum = 0;
            for (int j = 0; j < numStates; j++) {
                double a = 0;
                for (int i = 0; i < numStates; i++) a += alpha[t - 1][i] * trans[i][j];
                alpha[t][j] = a * emissionProb(observations[t], j);
                sum += alpha[t][j];
            }
            if (sum > 0) for (int j = 0; j < numStates; j++) alpha[t][j] /= sum;
        }
        return alpha;
    }

    /** Backward algorithm: returns beta[T][N]. */
    public double[][] backward(double[][] observations) {
        int T = observations.length;
        double[][] beta = new double[T][numStates];

        for (int i = 0; i < numStates; i++) beta[T - 1][i] = 1.0;

        for (int t = T - 2; t >= 0; t--) {
            double sum = 0;
            for (int i = 0; i < numStates; i++) {
                double b = 0;
                for (int j = 0; j < numStates; j++) {
                    b += trans[i][j] * emissionProb(observations[t + 1], j) * beta[t + 1][j];
                }
                beta[t][i] = b;
                sum += b;
            }
            if (sum > 0) for (int i = 0; i < numStates; i++) beta[t][i] /= sum;
        }
        return beta;
    }

    /** Viterbi: most likely state sequence. Returns int[T]. */
    public int[] viterbi(double[][] observations) {
        int T = observations.length;
        double[][] delta = new double[T][numStates];
        int[][] psi = new int[T][numStates];

        // Use log probabilities to avoid underflow
        for (int i = 0; i < numStates; i++) {
            delta[0][i] = Math.log(pi[i] + 1e-300) + Math.log(emissionProb(observations[0], i) + 1e-300);
        }

        for (int t = 1; t < T; t++) {
            for (int j = 0; j < numStates; j++) {
                double maxVal = Double.NEGATIVE_INFINITY;
                int argmax = 0;
                for (int i = 0; i < numStates; i++) {
                    double val = delta[t - 1][i] + Math.log(trans[i][j] + 1e-300);
                    if (val > maxVal) { maxVal = val; argmax = i; }
                }
                delta[t][j] = maxVal + Math.log(emissionProb(observations[t], j) + 1e-300);
                psi[t][j] = argmax;
            }
        }

        // Backtrace
        int[] path = new int[T];
        double maxVal = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < numStates; i++) {
            if (delta[T - 1][i] > maxVal) { maxVal = delta[T - 1][i]; path[T - 1] = i; }
        }
        for (int t = T - 2; t >= 0; t--) path[t] = psi[t + 1][path[t + 1]];
        return path;
    }

    /**
     * Compute posterior probability of current state given last observation.
     * Returns double[numStates].
     */
    public double[] getCurrentPosteriors(double[][] observations) {
        double[][] alpha = forward(observations);
        double[][] beta = backward(observations);
        int T = observations.length;
        double[] result = new double[numStates];
        double sum = 0;
        for (int i = 0; i < numStates; i++) {
            result[i] = alpha[T - 1][i] * beta[T - 1][i];
            sum += result[i];
        }
        if (sum > 0) for (int i = 0; i < numStates; i++) result[i] /= sum;
        return result;
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private double emissionProb(double[] obs, int state) {
        // Multivariate Gaussian with diagonal covariance
        double prob = 1.0;
        for (int d = 0; d < numFeatures; d++) {
            double diff = obs[d] - means[state][d];
            double var = variances[state][d];
            prob *= Math.exp(-0.5 * diff * diff / var) / Math.sqrt(2 * Math.PI * var);
        }
        return Math.max(prob, 1e-300); // floor to avoid underflow
    }

    private void initializeParameters(double[][] observations) {
        pi = new double[numStates];
        trans = new double[numStates][numStates];
        means = new double[numStates][numFeatures];
        variances = new double[numStates][numFeatures];

        // Initial state: uniform
        for (int i = 0; i < numStates; i++) pi[i] = 1.0 / numStates;

        // Transition matrix: sticky (90% stay, rest split evenly)
        for (int i = 0; i < numStates; i++) {
            for (int j = 0; j < numStates; j++) {
                trans[i][j] = (i == j) ? 0.9 : 0.1 / (numStates - 1);
            }
        }

        // Initialize means using k-means-like approach:
        // spread initial means across observation range per feature
        double[] minVals = new double[numFeatures];
        double[] maxVals = new double[numFeatures];
        double[] globalMeans = new double[numFeatures];
        double[] globalVars = new double[numFeatures];

        for (int d = 0; d < numFeatures; d++) {
            minVals[d] = Double.POSITIVE_INFINITY;
            maxVals[d] = Double.NEGATIVE_INFINITY;
        }
        for (double[] obs : observations) {
            for (int d = 0; d < numFeatures; d++) {
                if (obs[d] < minVals[d]) minVals[d] = obs[d];
                if (obs[d] > maxVals[d]) maxVals[d] = obs[d];
                globalMeans[d] += obs[d];
            }
        }
        for (int d = 0; d < numFeatures; d++) globalMeans[d] /= observations.length;
        for (double[] obs : observations) {
            for (int d = 0; d < numFeatures; d++) {
                double diff = obs[d] - globalMeans[d];
                globalVars[d] += diff * diff;
            }
        }
        for (int d = 0; d < numFeatures; d++) {
            globalVars[d] = Math.max(globalVars[d] / observations.length, 1e-6);
        }

        // Spread means across the range of each feature
        for (int i = 0; i < numStates; i++) {
            double fraction = (i + 0.5) / numStates;
            for (int d = 0; d < numFeatures; d++) {
                means[i][d] = minVals[d] + fraction * (maxVals[d] - minVals[d]);
                // Add small jitter for asymmetry
                means[i][d] += (random.nextDouble() - 0.5) * 0.01 * (maxVals[d] - minVals[d]);
                variances[i][d] = globalVars[d];
            }
        }
    }
}
