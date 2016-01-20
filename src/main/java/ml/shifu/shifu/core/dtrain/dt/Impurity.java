/*
 * Copyright [2013-2015] PayPal Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.shifu.core.dtrain.dt;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ml.shifu.shifu.container.obj.ColumnConfig;

/**
 * TODO FOR categorigcal feature, do a shuffle in {@link #computeImpurity(double[], ColumnConfig)} firstly
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
public abstract class Impurity {

    protected int statsSize;

    public abstract GainInfo computeImpurity(double[] stats, ColumnConfig confg);

    public abstract void featureUpdate(double[] featuerStatistic, int binIndex, float label, float significance,
            float weight);

    /**
     * @return the statsSize
     */
    public int getStatsSize() {
        return statsSize;
    }

    /**
     * @param statsSize
     *            the statsSize to set
     */
    public void setStatsSize(int statsSize) {
        this.statsSize = statsSize;
    }

}

class Variance extends Impurity {

    public Variance() {
        // 3 are count, sum and sumSquare
        super.statsSize = 3;
    }

    @Override
    public GainInfo computeImpurity(double[] stats, ColumnConfig config) {
        double count = 0d, sum = 0d, sumSquare = 0d;
        for(int i = 0; i < stats.length / super.statsSize; i++) {
            count += stats[i * super.statsSize];
            sum += stats[i * super.statsSize + 1];
            sumSquare += stats[i * super.statsSize + 2];
        }

        double impurity = getImpurity(count, sum, sumSquare);
        Predict predict = new Predict(sum / count);

        double leftCount = 0d, leftSum = 0d, leftSumSquare = 0d;
        double rightCount = 0d, rightSum = 0d, rightSumSquare = 0d;
        List<GainInfo> internalGainList = new ArrayList<GainInfo>();
        Set<String> leftCategories = config.isCategorical() ? new HashSet<String>() : null;
        for(int i = 0; i < stats.length / super.statsSize; i++) {
            leftCount += stats[i * super.statsSize];
            leftSum += stats[i * super.statsSize + 1];
            leftSumSquare += stats[i * super.statsSize + 2];

            rightCount = count - leftCount;
            rightSum = sum - leftSum;
            rightSumSquare = sumSquare - leftSumSquare;

            double leftWeight = leftCount / count;
            double rightWeight = rightCount / count;
            double leftImpurity = getImpurity(leftCount, leftSum, leftSumSquare);
            double rightImpurity = getImpurity(rightCount, rightSum, rightSumSquare);
            double gain = impurity - leftWeight * leftImpurity - rightWeight * rightImpurity;

            Split split = null;
            if(config.isCategorical()) {
                leftCategories.add(config.getBinCategory().get(i));
                split = new Split(config.getColumnNum(), FeatureType.CATEGORICAL, 0d, leftCategories);
            } else {
                split = new Split(config.getColumnNum(), FeatureType.CONTINUOUS, config.getBinBoundary().get(i), null);
            }

            Predict leftPredict = new Predict(leftSum / leftCount);
            Predict rightPredict = new Predict(rightSum / rightCount);

            internalGainList.add(new GainInfo(gain, impurity, predict, leftImpurity, rightImpurity, leftPredict,
                    rightPredict, split));
        }
        return GainInfo.getGainInfoByMaxGain(internalGainList);
    }

    private double getImpurity(double count, double sum, double sumSquare) {
        return (count != 0d) ? ((sumSquare - (sum * sum) / count) / count) : 0d;
    }

    @Override
    public void featureUpdate(double[] featuerStatistic, int binIndex, float label, float significance, float weight) {
        featuerStatistic[binIndex * super.statsSize] += significance * weight;
        featuerStatistic[binIndex * super.statsSize + 1] += label * significance * weight;
        featuerStatistic[binIndex * super.statsSize + 2] += label * label * significance * weight;
    }

}

class Entropy extends Impurity {

    public Entropy(int numClasses) {
        assert numClasses > 0;
        super.statsSize = numClasses;
    }

    @Override
    public GainInfo computeImpurity(double[] stats, ColumnConfig config) {
        int numClasses = super.statsSize;
        double[] statsByClasses = new double[numClasses];

        for(int i = 0; i < stats.length / numClasses; i++) {
            for(int j = 0; j < numClasses; j++) {
                double oneStatValue = stats[i * super.statsSize + j];
                statsByClasses[j] += oneStatValue;
            }
        }

        InternalEntropyInfo info = getEntropyInterInfo(statsByClasses);
        // prob only effective in binary classes
        Predict predict = new Predict(info.indexOfLagestElement, statsByClasses[1] / info.sumAll);

        double[] leftStatByClasses = new double[numClasses];
        double[] rightStatByClasses = new double[numClasses];
        List<GainInfo> internalGainList = new ArrayList<GainInfo>();
        Set<String> leftCategories = config.isCategorical() ? new HashSet<String>() : null;
        for(int i = 0; i < stats.length / numClasses; i++) {
            for(int j = 0; j < leftStatByClasses.length; j++) {
                leftStatByClasses[j] += stats[i * numClasses + j];
            }
            InternalEntropyInfo leftInfo = getEntropyInterInfo(leftStatByClasses);
            Predict leftPredict = new Predict(leftInfo.indexOfLagestElement, leftStatByClasses[1] / leftInfo.sumAll);

            for(int j = 0; j < leftStatByClasses.length; j++) {
                rightStatByClasses[j] = statsByClasses[j] - leftStatByClasses[j];
            }
            InternalEntropyInfo rightInfo = getEntropyInterInfo(rightStatByClasses);
            Predict rightPredict = new Predict(rightInfo.indexOfLagestElement, rightStatByClasses[1] / rightInfo.sumAll);

            double gain = info.impurity - (leftInfo.sumAll / info.sumAll) * leftInfo.impurity
                    - (rightInfo.sumAll / info.sumAll) * rightInfo.impurity;
            Split split = null;
            if(config.isCategorical()) {
                leftCategories.add(config.getBinCategory().get(i));
                split = new Split(config.getColumnNum(), FeatureType.CATEGORICAL, 0d, leftCategories);
            } else {
                split = new Split(config.getColumnNum(), FeatureType.CONTINUOUS, config.getBinBoundary().get(i), null);
            }

            internalGainList.add(new GainInfo(gain, info.impurity, predict, leftInfo.impurity, rightInfo.impurity,
                    leftPredict, rightPredict, split));
        }
        return GainInfo.getGainInfoByMaxGain(internalGainList);
    }

    private InternalEntropyInfo getEntropyInterInfo(double[] statsByClasses) {
        double sumAll = 0;
        for(int i = 0; i < statsByClasses.length; i++) {
            sumAll += statsByClasses[i];
        }

        double impurity = 0d;
        int indexOfLargestElement = 0;
        double maxElement = Double.MIN_VALUE;
        for(int i = 0; i < statsByClasses.length; i++) {
            double rate = statsByClasses[i] / sumAll;
            impurity -= rate * log2(rate);
            if(statsByClasses[i] > maxElement) {
                maxElement = statsByClasses[i];
                indexOfLargestElement = i;
            }
        }

        return new InternalEntropyInfo(sumAll, indexOfLargestElement, impurity);
    }

    private static class InternalEntropyInfo {
        double sumAll;
        double indexOfLagestElement;
        double impurity;

        public InternalEntropyInfo(double sumAll, double indexOfLagestElement, double impurity) {
            this.sumAll = sumAll;
            this.indexOfLagestElement = indexOfLagestElement;
            this.impurity = impurity;
        }
    }

    @Override
    public void featureUpdate(double[] featuerStatistic, int binIndex, float label, float significance, float weight) {
        // label + 0.1f to avoid 0.99999f is converted to 0
        featuerStatistic[binIndex * super.statsSize + (int) (label + 0.1f)] += significance * weight;
    }

    private double log2(double x) {
        return Math.log(x) / Math.log(2);
    }

}

class Gini extends Impurity {

    public Gini(int numClasses) {
        assert numClasses > 0;
        super.statsSize = numClasses;
    }

    @Override
    public GainInfo computeImpurity(double[] stats, ColumnConfig config) {
        int numClasses = super.statsSize;
        double[] statsByClasses = new double[numClasses];

        for(int i = 0; i < stats.length / numClasses; i++) {
            for(int j = 0; j < numClasses; j++) {
                double oneStatValue = stats[i * super.statsSize + j];
                statsByClasses[j] += oneStatValue;
            }
        }

        InternalEntropyInfo info = getEntropyInterInfo(statsByClasses);
        // prob only effective in binary classes
        Predict predict = new Predict(info.indexOfLagestElement, statsByClasses[1] / info.sumAll);

        double[] leftStatByClasses = new double[numClasses];
        double[] rightStatByClasses = new double[numClasses];
        List<GainInfo> internalGainList = new ArrayList<GainInfo>();
        Set<String> leftCategories = config.isCategorical() ? new HashSet<String>() : null;
        for(int i = 0; i < stats.length / numClasses; i++) {
            for(int j = 0; j < leftStatByClasses.length; j++) {
                leftStatByClasses[j] += stats[i * numClasses + j];
            }
            InternalEntropyInfo leftInfo = getEntropyInterInfo(leftStatByClasses);
            Predict leftPredict = new Predict(leftInfo.indexOfLagestElement, leftStatByClasses[1] / leftInfo.sumAll);

            for(int j = 0; j < leftStatByClasses.length; j++) {
                rightStatByClasses[j] = statsByClasses[j] - leftStatByClasses[j];
            }
            InternalEntropyInfo rightInfo = getEntropyInterInfo(rightStatByClasses);
            Predict rightPredict = new Predict(rightInfo.indexOfLagestElement, rightStatByClasses[1] / rightInfo.sumAll);

            double gain = info.impurity - (leftInfo.sumAll / info.sumAll) * leftInfo.impurity
                    - (rightInfo.sumAll / info.sumAll) * rightInfo.impurity;
            Split split = null;
            if(config.isCategorical()) {
                leftCategories.add(config.getBinCategory().get(i));
                split = new Split(config.getColumnNum(), FeatureType.CATEGORICAL, 0d, leftCategories);
            } else {
                split = new Split(config.getColumnNum(), FeatureType.CONTINUOUS, config.getBinBoundary().get(i), null);
            }

            internalGainList.add(new GainInfo(gain, info.impurity, predict, leftInfo.impurity, rightInfo.impurity,
                    leftPredict, rightPredict, split));
        }
        return GainInfo.getGainInfoByMaxGain(internalGainList);
    }

    private InternalEntropyInfo getEntropyInterInfo(double[] statsByClasses) {
        double sumAll = 0;
        for(int i = 0; i < statsByClasses.length; i++) {
            sumAll += statsByClasses[i];
        }

        double impurity = 0d;
        int indexOfLargestElement = 0;
        double maxElement = Double.MIN_VALUE;
        for(int i = 0; i < statsByClasses.length; i++) {
            double rate = statsByClasses[i] / sumAll;
            impurity -= rate * rate;
            if(statsByClasses[i] > maxElement) {
                maxElement = statsByClasses[i];
                indexOfLargestElement = i;
            }
        }

        return new InternalEntropyInfo(sumAll, indexOfLargestElement, impurity);
    }

    private static class InternalEntropyInfo {
        double sumAll;
        double indexOfLagestElement;
        double impurity;

        public InternalEntropyInfo(double sumAll, double indexOfLagestElement, double impurity) {
            this.sumAll = sumAll;
            this.indexOfLagestElement = indexOfLagestElement;
            this.impurity = impurity;
        }
    }

    @Override
    public void featureUpdate(double[] featuerStatistic, int binIndex, float label, float significance, float weight) {
        // label + 0.1f to avoid 0.99999f is converted to 0
        featuerStatistic[binIndex * super.statsSize + (int) (label + 0.1f)] += significance * weight;
    }

}
