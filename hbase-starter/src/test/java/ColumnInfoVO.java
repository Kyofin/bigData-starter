import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @program: bigdata-starter
 * @author: huzekang
 * @create: 2019-10-12 11:10
 **/
@Data
@AllArgsConstructor
public class ColumnInfoVO {
    private String displayColumnName;
    private int sqlType;
    private String sqlTypeName;

}
