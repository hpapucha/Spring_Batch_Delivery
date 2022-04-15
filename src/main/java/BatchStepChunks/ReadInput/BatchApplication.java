package BatchStepChunks.ReadInput;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.util.List;

@SpringBootApplication
public class BatchApplication {

	public static String[] tokens = new String[] {"order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"};

	public static String ORDER_SQL = "select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date "
			+ "from SHIPPED_ORDER order by order_id";

	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	@Autowired
	public DataSource dataSource;



	@Bean public PagingQueryProvider queryProvider() throws Exception{
		SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
		factory.setSelectClause("select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date");
		factory.setFromClause("from SHIPPED_ORDER");
		factory.setSortKey("order_id");
		factory.setDataSource(dataSource);
		return factory.getObject();
	}
	// Allows itemreader to connect to db thread safe need to specify chunk size
	@Bean
	public ItemReader<Order> itemReader() throws Exception {
		return new JdbcPagingItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.queryProvider(queryProvider())
				.rowMapper(new OrderRowMapper())
				.build();
	}


	//Allows itemreader to connect to db not thread safe
	@Bean
	public ItemReader<Order> itemReader(){
		return new JdbcCursorItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.sql(ORDER_SQL)
				//RowMapper maps rows from db to our pojo
				.rowMapper(new OrderRowMapper())
				.build();
	}


	//Read data from csv file
	@Bean
	public ItemReader<Order> itemReader(){
		FlatFileItemReader<Order> itemReader = new FlatFileItemReader<Order>();
		//skip first line that contains headers
		itemReader.setLinesToSkip(1);
		//finding the data file
		itemReader.setResource(new FileSystemResource("/data/shipped_orders.csv"));
		//Proccesing data as it reads it with defaultlinemapper
		DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<Order>();
		//This breaks lines by commas as delimiter
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		//Tell the name of the columns by passing an array
		tokenizer.setNames(tokens);
		//Set line tokenizer on our line mapper
		lineMapper.setLineTokenizer(tokenizer);
		//Create order object passed by delimitedLineTokenizer
		lineMapper.setFieldSetMapper(new OrderFieldSetMapper());
		//Set lineMapper on our itemReader
		itemReader.setLineMapper(lineMapper);
		return itemReader;

	}
	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep")//supplying chunk
				.<Order, Order>chunk(3)
				.reader(itemReader())
				.writer(new ItemWriter<Order>() {

					@Override
					public void write(List<? extends Order> items) throws Exception {
						System.out.println(String.format("Receiced a list of size: %s" , items.size()));
						items.forEach(System.out::println);
					}
				}).build();
	}

	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}

}
