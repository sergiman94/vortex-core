
package com.vortex.vortexdb.analyzer;

import com.vortex.common.config.ConfigException;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.vortexdb.VortexException;
import com.chenlb.mmseg4j.*;
import com.google.common.collect.ImmutableList;

import java.io.StringReader;
import java.util.List;
import java.util.Set;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class MMSeg4JAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "Simple",
            "Complex",
            "MaxWord"
    );

    private static final Dictionary DIC = Dictionary.getInstance();

    private Seg seg;

    public MMSeg4JAnalyzer(String mode) {
        if (!SUPPORT_MODES.contains(mode)) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for mmseg4j analyzer, " +
                      "the available values are %s", mode, SUPPORT_MODES);
        }
        int index = SUPPORT_MODES.indexOf(mode);
        switch (index) {
            case 0:
                this.seg = new SimpleSeg(DIC);
                break;
            case 1:
                this.seg = new ComplexSeg(DIC);
                break;
            case 2:
                this.seg = new MaxWordSeg(DIC);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported segment mode '%s'", this.seg));
        }
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        MMSeg mmSeg = new MMSeg(new StringReader(text), this.seg);
        try {
            Word word = null;
            while ((word = mmSeg.next()) != null) {
                result.add(word.getString());
            }
        } catch (Exception e) {
            throw new VortexException("MMSeg4j segment text '%s' failed",
                                    e, text);
        }
        return result;
    }
}
