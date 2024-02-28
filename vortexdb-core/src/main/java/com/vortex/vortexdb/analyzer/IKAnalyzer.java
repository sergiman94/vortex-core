
package com.vortex.vortexdb.analyzer;

import com.vortex.common.config.ConfigException;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.vortexdb.VortexException;
import com.google.common.collect.ImmutableList;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.List;
import java.util.Set;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class IKAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "smart",
            "max_word"
    );

    private boolean smartSegMode;

    public IKAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for ikanalyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        this.smartSegMode = SUPPORT_MODES.get(0).equals(mode);
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        IKSegmenter ik = new IKSegmenter(new StringReader(text),
                                         this.smartSegMode);
        try {
            Lexeme word = null;
            while ((word = ik.next()) != null) {
                result.add(word.getLexemeText());
            }
        } catch (Exception e) {
            throw new VortexException("IKAnalyzer segment text '%s' failed",
                                    e, text);
        }
        return result;
    }
}
