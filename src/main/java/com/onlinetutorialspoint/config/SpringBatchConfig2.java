package com.onlinetutorialspoint.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig2 {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired 
    public DataSource dataSource;

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public JobExplorer jobExplorer() throws Exception {
        MapJobExplorerFactoryBean jobExplorerFactory = new MapJobExplorerFactoryBean(mapJobRepositoryFactoryBean());
        jobExplorerFactory.afterPropertiesSet();
        return jobExplorerFactory.getObject();
    }

    @Bean
    public MapJobRepositoryFactoryBean mapJobRepositoryFactoryBean() {
        MapJobRepositoryFactoryBean mapJobRepositoryFactoryBean = new MapJobRepositoryFactoryBean();
        mapJobRepositoryFactoryBean.setTransactionManager(transactionManager());
        return mapJobRepositoryFactoryBean;
    }

    @Bean
    public JobRepository jobRepository() throws Exception {
        return mapJobRepositoryFactoryBean().getObject();
    }

    @Bean
    public JobLauncher jobLauncher() throws Exception {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository());
        return simpleJobLauncher;
    }
    
//    public ItemReader<Employee> reader(DataSource datasource) {
//        JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<Employee>();
//        reader.setDataSource(datasource);
//        reader.setName("project-reader");
//        reader.setSql("SELECT id, name, description, creationDate, createdBy FROM dbo.project");
//        reader.setRowMapper(new BeanPropertyRowMapper<>(Employee.class));
//        reader.setFetchSize(100);
//        return reader;      
//    }

    @Bean
    public FlatFileItemReader<Employee> reader() {
        FlatFileItemReader<Employee> reader = new FlatFileItemReader<Employee>();
        reader.setResource(new ClassPathResource("employee.csv"));

        reader.setLineMapper(new DefaultLineMapper<Employee>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "first_name", "last_name","company_name","address","city","county","state","zip" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper() {{
                setTargetType(Employee.class);
            }});
        }});
        return reader;
    } 
    
    @Bean
    public EmployeeProcessor processor() {
        return new EmployeeProcessor();
    }

    @Bean
    public ItemWriter<EmployeeDTO> jdbcWriter(DataSource ds, ItemPreparedStatementSetter<EmployeeDTO> setter) {

        JdbcBatchItemWriter<EmployeeDTO> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(ds);           
        writer.setSql("INSERT INTO employee_preprocess(first_name, last_name, company_name, address, city, county, state, zip) VALUES(?,?,?,?,?,?,?,?);");
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setItemPreparedStatementSetter(setter);

        return writer;

    }

    @Bean
    public ItemPreparedStatementSetter<EmployeeDTO> setter() {
        return (item, ps) -> {
            ps.setString(1, item.getFirstName());
            ps.setString(2, item.getLastName());
            ps.setString(3, item.getCompanyName());
            ps.setString(4, item.getAddress());
            ps.setString(5, item.getCity());
            ps.setString(6, item.getCounty());
            ps.setString(7, item.getState());
            ps.setString(8, item.getZip());
         
        };
    }


    @Bean
    public Job importProjectTableJob(JobListener listener) {
        return jobBuilderFactory.get("importFortifyProjectTable")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(databaseOpsTasklet("DELETE FROM employee_preprocess"))
                .next(loadIntoProcessingTable())
                .next(databaseOpsTasklet("INSERT INTO employee_backup SELECT * FROM employee"))
                .next(databaseOpsTasklet("DELETE FROM employee"))
                .next(databaseOpsTasklet("INSERT INTO employee SELECT * FROM employee_preprocess"))                
                .build();
    }

    public Step databaseOpsTasklet(String sql){
        return stepBuilderFactory.get("loadIntoProcessingTable")
                .tasklet(tasklet(sql))
                .build();       
    }


    public Tasklet tasklet(String sql) {
        return new TableOpsTasklet(sql, dataSource);
    }

    @Bean
    public Step loadIntoProcessingTable() {
        return stepBuilderFactory.get("loadIntoProcessingTable")
                .<Employee, EmployeeDTO> chunk(100)
                .reader(reader())
                .processor(processor())
                .writer(jdbcWriter(dataSource, setter()))
                .build();
    }

    /*
    @Bean
    public JobRunScheduler scheduler() {
        JobRunScheduler scheduler = new JobRunScheduler();
        return scheduler;
    }
   */
    // end::jobstep[]
}