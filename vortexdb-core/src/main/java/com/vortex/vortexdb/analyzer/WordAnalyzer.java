
package com.vortex.vortexdb.analyzer;

import com.google.common.collect.ImmutableList;
import com.vortex.common.config.ConfigException;
import com.vortex.common.util.InsertionOrderUtil;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.SegmentationAlgorithm;
import org.apdplat.word.segmentation.Word;

import java.util.List;
import java.util.Set;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class WordAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES =
           ImmutableList.<String>builder()
                        .add("MaximumMatching")
                        .add("ReverseMaximumMatching")
                        .add("MinimumMatching")
                        .add("ReverseMinimumMatching")
                        .add("BidirectionalMaximumMatching")
                        .add("BidirectionalMinimumMatching")
                        .add("BidirectionalMaximumMinimumMatching")
                        .add("FullSegmentation")
                        .add("MinimalWordCount")
                        .add("MaxNgramScore")
                        .add("PureEnglish")
                        .build();

    private SegmentationAlgorithm algorithm;

    public WordAnalyzer(String mode) {
        try {
            this.algorithm = SegmentationAlgorithm.valueOf(mode);
        } catch (Exception e) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for word analyzer, " +
                      "the available values are %s", e, mode, SUPPORT_MODES);
        }
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        List<Word> words = WordSegmenter.segWithStopWords(text, this.algorithm);
        for (Word word : words) {
            result.add(word.getText());
        }
        return result;
    }
}
