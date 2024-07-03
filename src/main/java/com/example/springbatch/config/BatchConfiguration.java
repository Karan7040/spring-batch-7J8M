package com.example.springbatch.config;

import com.example.springbatch.model.Employee;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration {
    @Value("${file.input}") // To fetch data from yaml
    String fileinput;

    @Bean
    public FlatFileItemReader<Employee> reader() {
        FlatFileItemReaderBuilder<Employee> flatFileItemReaderBuilder = new FlatFileItemReaderBuilder<>();
        return flatFileItemReaderBuilder.name("reader").resource(new ClassPathResource(fileinput)).delimited()
                .names(new String[]{"id", "name", "designation", "phoneno"}).fieldSetMapper(new BeanWrapperFieldSetMapper<>() {
                    {
                        setTargetType(Employee.class);
                    }
                }).build();
    }

    @Bean
    public JdbcBatchItemWriter<Employee> writer(DataSource dataSource) {
        JdbcBatchItemWriterBuilder<Employee> jdbcBatchItemWriterBuilder = new JdbcBatchItemWriterBuilder<>();
        return jdbcBatchItemWriterBuilder.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("Insert into EMPLOYEE values(:id, :name, :designation, :phoneno)").dataSource(dataSource).build();

    }

    @Bean
    public Step step(JobRepository jobRepository,
                     PlatformTransactionManager platformTransactionManager,
                     DataSource dataSource) {
        StepBuilder stepBuilder = new StepBuilder("Step", jobRepository);
        return stepBuilder.<Employee, Employee>chunk(2, platformTransactionManager)
                .reader(reader()).writer(writer(dataSource)).build();

    }


    @Bean
    public Job importUserJob(JobRepository jobRepository, Step step) {
        return new JobBuilder("importUserJob", jobRepository)
                .start(step)
                .build();
    }


}
