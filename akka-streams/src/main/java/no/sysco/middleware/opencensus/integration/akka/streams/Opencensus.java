package no.sysco.middleware.opencensus.integration.akka.streams;

import akka.stream.*;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import io.opencensus.trace.BlankSpan;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;

public class Opencensus {

    static <A, B, Mat> Flow<A, B, Mat> traceFlow(String spanName, Graph<FlowShape<A, B>, Mat> flow) {
        Graph<FlowShape<A, B>, Mat> flowGraph =
                GraphDSL.create(flow, (builder, flowShape) -> {
                    BidiShape<A, A, B, B> spanBidiShape = builder.add(new SpanBidiStage<>(spanName));
                    builder.from(spanBidiShape.out1()).via(flowShape).toInlet(spanBidiShape.in2());
                    return FlowShape.of(spanBidiShape.in1(), spanBidiShape.out2());
                });
        return Flow.fromGraph(flowGraph);
    }

    static class SpanBidiStage<A, B> extends GraphStage<BidiShape<A, A, B, B>> {

        private final Inlet<A> inlet1 = Inlet.create("SpanBidiStage.in1");
        private final Outlet<A> outlet1 = Outlet.create("SpanBidiStage.out1");
        private final Inlet<B> inlet2 = Inlet.create("SpanBidiStage.in2");
        private final Outlet<B> outlet2 = Outlet.create("SpanBidiStage.out2");

        private final BidiShape<A, A, B, B> bidiShape = new BidiShape<>(inlet1, outlet1, inlet2, outlet2);

        private final String spanName;
        private final Sampler sampler;
        private final boolean recordEvents;

        SpanBidiStage(String spanName, Sampler sampler, boolean recordEvents) {
            this.spanName = spanName;
            this.sampler = sampler;
            this.recordEvents = recordEvents;
        }

        SpanBidiStage(String spanName) {
            this.spanName = spanName;
            this.sampler = Samplers.alwaysSample();
            this.recordEvents = true;
        }

        @Override
        public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
            return new GraphStageLogic(bidiShape) {
                private Span span = BlankSpan.INSTANCE;

                {
                    setHandler(inlet1, new AbstractInHandler() {
                        @Override
                        public void onPush() {
                            try {
                                span = Tracing.getTracer()
                                        .spanBuilder(spanName)
                                        .setSampler(sampler)
                                        .setRecordEvents(recordEvents)
                                        .startSpan();
                            } catch (Throwable e) {

                            }
                            push(outlet1, grab(inlet1));
                        }
                    });

                    setHandler(outlet1, new AbstractOutHandler() {
                        @Override
                        public void onPull() {
                            pull(inlet1);
                        }
                    });

                    setHandler(inlet2, new AbstractInHandler() {
                        @Override
                        public void onPush() {
                            span.end();
                            push(outlet2, grab(inlet2));
                        }
                    });

                    setHandler(outlet2, new AbstractOutHandler() {
                        @Override
                        public void onPull() {
                            pull(inlet2);
                        }
                    });
                }
            };
        }

        @Override
        public BidiShape<A, A, B, B> shape() {
            return bidiShape;
        }
    }
}