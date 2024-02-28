
package com.vortex.vortexdb.analyzer;

import com.google.common.collect.ImmutableList;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.vortex.common.config.ConfigException;
import com.vortex.common.util.InsertionOrderUtil;

import java.util.List;
import java.util.Set;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class JiebaAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "SEARCH",
            "INDEX"
    );

    private static final JiebaSegmenter JIEBA_SEGMENTER = new JiebaSegmenter();

    private JiebaSegmenter.SegMode segMode;

    public JiebaAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for jieba analyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        this.segMode = JiebaSegmenter.SegMode.valueOf(mode);
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        for (SegToken token : JIEBA_SEGMENTER.process(text, this.segMode)) {
            result.add(token.word);
        }
        return result;
    }
}
