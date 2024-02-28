
package com.vortex.vortexdb.analyzer;

import com.google.common.collect.ImmutableList;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.vortexdb.VortexException;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Set;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class SmartCNAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of();

    private static final SmartChineseAnalyzer ANALYZER =
                                              new SmartChineseAnalyzer();

    public SmartCNAnalyzer(String mode) {
        // pass
    }

    @Override
    public Set<String> segment(String text) {
        Set<String> result = InsertionOrderUtil.newSet();
        Reader reader = new StringReader(text);
        try (TokenStream tokenStream = ANALYZER.tokenStream("text", reader)) {
            tokenStream.reset();
            CharTermAttribute term = null;
            while (tokenStream.incrementToken()) {
                term = tokenStream.getAttribute(CharTermAttribute.class);
                result.add(term.toString());
            }
        } catch (Exception e) {
            throw new VortexException("SmartCN segment text '%s' failed",
                                    e, text);
        }
        return result;
    }
}
