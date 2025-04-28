module flinkCEP.Patterns {
    requires kryo;
    requires java.sql;
    requires io.github.ericmedvet.jgea.core;
    requires io.github.ericmedvet.jgea.experimenter;
    requires io.github.ericmedvet.jnb.core;
    requires org.apache.commons.csv;
    requires flink.cep;
    requires merged.jar;
    requires java.desktop;
    requires org.slf4j; 

    opens events to kryo;
    opens problem to io.github.ericmedvet.jnb.core;
    opens cep to flink.cep, merged.jar;
}
