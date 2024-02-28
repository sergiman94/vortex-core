
package com.vortex.vortexdb.analyzer;

import com.google.common.collect.ImmutableList;
import com.vortex.common.config.ConfigException;
import com.vortex.common.util.InsertionOrderUtil;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.IndexAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.List;
import java.util.Set;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class AnsjAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "BaseAnalysis",
            "IndexAnalysis",
            "ToAnalysis",
            "NlpAnalysis"
    );

    private String analysis;

    public AnsjAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for ansj analyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        this.analysis = mode;
    }

    @Override
    public Set<String> segment(String text) {
        Result terms = null;
        switch (this.analysis) {
            case "BaseAnalysis":
                terms = BaseAnalysis.parse(text);
                break;
            case "ToAnalysis":
                terms = ToAnalysis.parse(text);
                break;
            case "NlpAnalysis":
                terms = NlpAnalysis.parse(text);
                break;
            case "IndexAnalysis":
                terms = IndexAnalysis.parse(text);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported segment mode '%s'", this.analysis));
        }

        assert terms != null;
        Set<String> result = InsertionOrderUtil.newSet();
        for (Term term : terms) {
            result.add(term.getName());
        }
        return result;
    }
}
