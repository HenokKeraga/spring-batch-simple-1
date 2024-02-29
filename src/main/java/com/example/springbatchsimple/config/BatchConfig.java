package com.example.springbatchsimple.config;

import com.example.springbatchsimple.model.Student;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;
import java.util.List;

@Configuration
@EnableConfigurationProperties(BatchProperties.class)
@RequiredArgsConstructor
public class BatchConfig extends DefaultBatchConfiguration {
    final BatchProperties batchProperties;
    final DataSource dataSource;

    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceScriptDatabaseInitializer() {
        return new BatchDataSourceScriptDatabaseInitializer(getDataSource(), batchProperties.getJdbc());
    }

    @Bean
    public Job job(Step step) {

        return new JobBuilder("job", jobRepository())

                .start(step)
                .build();

    }

    @Bean
    public Step step(ItemReader<Student> flatFileItemReader, ItemWriter<Student> jdbcBatchItemWriter, ItemProcessor<Student, Student> processor) {

        return new StepBuilder("step", jobRepository())
                //.<String, String>chunk(2, getTransactionManager())
                .<Student, Student>chunk(2, getTransactionManager())
                .reader(flatFileItemReader)
                .processor(processor)
                .writer(jdbcBatchItemWriter)
                .build();
    }

    @Bean(name = "flatFileItemReader")
    public FlatFileItemReader<Student> flatFileItemReader() {
        FlatFileItemReader<Student> reader = new FlatFileItemReader<>();
        reader.setName("flatFileItemReader");
        reader.setResource(new ClassPathResource("students.csv"));

        DefaultLineMapper<Student> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setNames("id", "name", "department", "age");

        BeanWrapperFieldSetMapper<Student> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Student.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);


        reader.setLineMapper(lineMapper);
        return reader;

    }

    @Bean
    public ItemReader<String> itemReader() {

        return new ItemReader<>() {
            final List<String> data = List.of("A", "B", "J", "G", "H");
            int count = -1;

            @Override
            public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                count++;
                if (count < 5)
                    return data.get(count);
                else
                    return null;
            }
        };
    }

    @Bean
    public ItemWriter<String> itemWriter() {

        return System.out::println;
    }

    @Bean
    public ItemWriter<Student> jdbcBatchItemWriter() {

        JdbcBatchItemWriter<Student> itemWriter = new JdbcBatchItemWriter<>();

        itemWriter.setDataSource(dataSource);
        itemWriter.setSql("INSERT INTO student (id,name,department,age) values (:id, :name, :department, :age)");
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());

        return itemWriter;
    }

    @Bean
    public ItemProcessor<Student, Student> processor() {

        return item -> {
            if (item.getId() < 10) {
                item.setAge(item.getAge() * 10);
                return item;
            }
            else
                return null;
        };
    }

}
