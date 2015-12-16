package eu.fbk.pikesir.lucene;

import java.io.IOException;

import com.google.common.primitives.Ints;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;

public class WeightAnalyzer extends Analyzer {

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        return new TokenStreamComponents(new WeightTokenizer());
    }

    private static final class WeightTokenizer extends Tokenizer {

        private final CharTermAttribute termAtt;

        private final OffsetAttribute offsetAtt;

        private final PayloadAttribute payloadAtt;

        private final char[] weightBuffer;

        private int finalOffset;

        private boolean done;

        public WeightTokenizer() {
            this.termAtt = addAttribute(CharTermAttribute.class);
            this.offsetAtt = addAttribute(OffsetAttribute.class);
            this.payloadAtt = addAttribute(PayloadAttribute.class);
            this.weightBuffer = new char[32];
            this.finalOffset = 0;
            this.done = false;
            this.termAtt.resizeBuffer(256);
        }

        @Override
        public final boolean incrementToken() throws IOException {
            if (!this.done) {
                clearAttributes();
                this.done = true;
                int upto = 0;
                char[] buffer = this.termAtt.buffer();

                float weight = 1.0f;
                int weightLen = 0;
                while (true) {
                    final int c = this.input.read();
                    if (c >= 0) {
                        this.weightBuffer[weightLen++] = (char) c;
                    }
                    if (c == ' ' && weightLen >= 3 && this.weightBuffer[weightLen - 2] == '|') {
                        weight = Float.parseFloat(new String(this.weightBuffer, 1, weightLen - 3));
                        break;
                    } else if (c < 0 || c != '|' && weightLen == 1 || c != '|' && c != '.'
                            && c != ' ' && !Character.isDigit(c)) {
                        System.arraycopy(this.weightBuffer, 0, buffer, 0, weightLen);
                        upto = weightLen;
                        break;
                    }
                }

                while (true) {
                    final int length = this.input.read(buffer, upto, buffer.length - upto);
                    if (length == -1) {
                        break;
                    }
                    upto += length;
                    if (upto == buffer.length) {
                        buffer = this.termAtt.resizeBuffer(1 + buffer.length);
                    }
                }

                this.termAtt.setLength(upto);
                this.finalOffset = correctOffset(upto);
                this.offsetAtt.setOffset(correctOffset(0), this.finalOffset);

                if (weight != 1.0f) {
                    final BytesRef payload = new BytesRef(Ints.toByteArray(Float
                            .floatToIntBits(weight)));
                    this.payloadAtt.setPayload(payload);
                }

                return true;
            }
            return false;
        }

        @Override
        public final void end() throws IOException {
            super.end();
            this.offsetAtt.setOffset(this.finalOffset, this.finalOffset);
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            this.done = false;
        }

    }

}
