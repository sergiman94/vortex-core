
package com.vortex.vortexdb.analyzer;

import com.vortex.common.config.ConfigException;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.vortexdb.VortexException;
import com.google.common.collect.ImmutableList;
import org.lionsoul.jcseg.tokenizer.core.*;

import java.io.StringReader;
import java.util.List;
import java.util.Set;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class JcsegAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "Simple",
            "Complex"
    );

    private static final JcsegTaskConfig CONFIG = new JcsegTaskConfig();
    private static final ADictionary DIC =
            DictionaryFactory.createDefaultDictionary(new JcsegTaskConfig());

    private int segMode;

    public JcsegAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for jcseg analyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        this.segMode = SUPPORT_MODES.indexOf(mode) + 1;
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        try {
            Object[] args = new Object[]{new StringReader(text), CONFIG, DIC};
            ISegment seg = SegmentFactory.createJcseg(this.segMode, args);
            IWord word = null;
            while ((word = seg.next()) != null) {
                result.add(word.getValue());
            }
        } catch (Exception e) {
            throw new VortexException("Jcseg segment text '%s' failed", e, text);
        }
        return result;
    }
}
