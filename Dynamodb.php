<?php
/**
 * dynamodb 操作类，对aws sdk的二次封装
 * 注意：dynamodb 对数据类型要求较严格，如果表字段是Number形，传String进去会查询不出
 * config示例
 $config = array(
	'passport' => array(
		'region'   => '',
		'version'  => '',
		'credentials' => array(
			'key'    => '',
			'secret' => '',
		),
	),
	
);
 */
require 'aws/aws-autoloader.php';
use Aws\DynamoDb\Exception\DynamoDbException;
use Aws\DynamoDb\Marshaler;

class Dynamodb{
	protected static $instance;
	protected $dynamodb;
	protected $marshaler;
	
	public static function getInstance($config){
		try{
			if(!self::$instance) {
				self::$instance = new self($dataSourceIndex);
			}
		}catch(Exception $e){
			throw new exception($e -> getMessage());
		}
		
		return self::$instance;
	}
	
	private function __construct($config){
		try{
			$sdk = new Aws\Sdk($config);
			$this -> dynamodb = $sdk->createDynamoDb();
			$this -> marshaler = new Marshaler();
		}catch(Exception $e){
			throw new exception($e -> getMessage());
		}
	}
	
	/**
	 * 建表,一般不使用，由运维创建
	 */
	public function createTable($tableContext){
		try{
			if(!is_array($tableContext) || !$tableContext['KeySchema']){
				throw new exception('table context not allow');
			}
			$res = $this -> dynamodb -> createTable($tableContext);
			if($res['TableDescription']['TableStatus'] == 'CREATING'){
				$result = true;
			}else{
				$result = false;
			}
		}catch(DynamoDbException $e){
			throw new Exception($e -> getMessage());
		}
		
		return $result;
	}
	
	/**
	 * 插入项目
	 */
	public function putItem($tableName, $item){
		try{
			$context = json_encode($item);
			$params = array(
				'TableName' => $tableName,
				'Item' => $this -> marshaler -> marshalJson($context)
			);
			$res = $this -> dynamodb -> putItem($params);
			if($res['@metadata']['statusCode'] == 200){
				$result = true;
			}else{
				$result = false;
			}
			return $result;
		}catch(DynamoDbException $e){
			throw new Exception($e -> getMessage());
		}
	}
	
	/**
	 * 读取项目,注意where条件必须指定主键值
	 * @param string $tableName
	 * @param array $where 
	 * @throws Exception
	 */
	public function getItem($tableName, $where){
		try{
			$json = json_encode($where);
			$key = $this -> marshaler -> marshalJson($json);
			$params = array(
				'TableName' => $tableName,
				'Key' => $key,
			);
			$res = $this -> dynamodb -> getItem($params);
			if(!empty($res) && $res['Item']){
				$result = $this -> marshaler -> unmarshalItem($res['Item']);
			}else{
				$result = array();
			}
			return $result;
		}catch(DynamoDbException $e){
			throw new Exception($e -> getMessage());
		}
	}
	
	/**
	 * 删除，主键一定要指定（推荐以主键为条件做删除操作）
	 * where 示例 array(
	 * 	'key1' => value1,
	 * 	'key2' => value2,
	 * )
	 * conditionStr 示例 此处的key1，key2 为实际库中的名字，:key1 :key2 为where中的key1, key2编码上尽量一直，dynamodb是为了避免关键字冲突才这样设计
	 * 'key1 == :key1 and key2 == :key2'
	 * @param string $tableName
	 * @param array $mainKey 主键
	 * @param array $where 非主键
	 * @param string $conditionStr 条件表达式
	 * @throws Exception
	 */
	public function delItem($tableName, $mainKey, $where = array(), $conditionStr = ''){
		try{
			$json = json_encode($mainKey);
			$keyStr = $this -> marshaler -> marshalJson($json);
			if(is_array($where)){
				foreach($where as $key => $value){
					$strKey = ":".$key;
					$eav[$strKey] = $value;
				}
			}
			$params = array(
				'TableName' => $tableName,
				'Key' => $keyStr,
			);
			if($eav && $conditionStr){
				$params['ConditionExpression'] = $conditionStr;
				$params['ExpressionAttributeValues'] = $eav;
			}
			
			$res = $this -> dynamodb -> deleteItem($params);
			return $res;
		}catch(DynamoDbException $e){
			throw new Exception($e -> getMessage());
		}
	}
	
	public function updateItem($tableName, $where, $field){
		try{
			//对Key组装
			$json = json_encode($where);
			$whereStr = $this -> marshaler -> marshalJson($json);
			
			//对ExpressionAttributeValues UpdateExpression 组装，可能会与dynamodb本身的关键字冲突，所以field里面的key值不能是dynamodb的关键字
			$num = count($field) - 1;
			$express = "set ";
			foreach($field as $key => $value){
				$strKey = ":".$key;
				$eavArr[$strKey] = $value;
				if($num > 0){
					$express .=  $key ." = " .$strKey . " , ";
				}else{
					$express .=  $key ." = " .$strKey;
				}
				$num--;				
			}
			$eavStr = json_encode($eavArr);
			$eav = $this -> marshaler -> marshalJson($eavStr);
			
			
			$params = array(
				'TableName' => $tableName,
				'Key' => $whereStr,
				'UpdateExpression' => $express,
				'ExpressionAttributeValues' => $eav,
				'ReturnValues' => 'UPDATED_NEW'
			);
			
			$res = $this -> dynamodb -> updateItem($params);
			$result = $this -> marshaler -> unmarshalItem($res['Attributes']);
			return $result;
		}catch(DynamoDbException $e){
			throw new Exception($e -> getMessage());
		}
	}
	
	
	
	/**
	 * 扫描，可以不用指定主键，性能会有影响
	 * @param string $tableName 表名
	 * @param array $where 查询条件
	 * @param string $filterExpression 查询表达式
	 * @param string $projectionExpression 返回数据项，默认全部
	 * @param array $expressionAttributeNames 当查询字段占用关键字可用该字段进行别名替换
	 * @throws Exception
	 * @return unknown
	 */
	public function scanItem($tableName, $field, $where, $projectionExpression = '', $expressionAttributeNames = array()){
		try{
			foreach($field as $key => $value){
				$strKey = ":".$key;
				$attrValue[$strKey] = $value;
			}
			$json = json_encode($attrValue);
			$expressionAttributeValues = $this -> marshaler -> marshalJson($json);
			$params = array(
				'TableName' => $tableName,
				'FilterExpression' => $where,	
				'ExpressionAttributeValues' => $expressionAttributeValues
			);
			if($projectionExpression){
				$params['ProjectionExpression'] = $projectionExpression;
			}
			if(empty($expressionAttributeValues)){
				$params['ExpressionAttributeNames'] = $expressionAttributeNames;
			}
			$res = $this -> dynamodb -> scan($params);
			foreach($res['Items'] as $i){
				$tmp = $this -> marshaler -> unmarshalItem($i);
				$result[] = $tmp;
			}
			return $result;
		}catch(DynamoDbException $e){
			throw new Exception($e -> getMessage());
		}
	}
	
	/**
	 * 查询，必须提供分区健，效率比扫描高
	 * @param string $tableName
	 * @param array $where 查询条件
	 * @param string $keyConditionExpression 查询表达式
	 * @param string $projectionExpression 返回项目
	 * @param string $expressionAttributeNames 当查询字段占用关键字可用该字段进行别名替换
	 * @throws Exception 
	 */
	public function queryItem($tableName, $where, $keyConditionExpression, $projectionExpression = '', $ExpressionAttributeNames = ''){
		try{
			$json = json_encode($where);
			$expressionAttributeValues = $this -> marshaler -> marshalJson($json);
			$params = array(
				'TableName' => $tableName,
				'KeyConditionExpression' => $keyConditionExpression,
				'ExpressionAttributeValues' => $expressionAttributeValues
			);
			if($projectionExpression){
				$params['ProjectionExpression'] = $projectionExpression;
			}
			if(empty($expressionAttributeValues)){
				$params['ExpressionAttributeNames'] = $expressionAttributeNames;
			}
			$res = $this -> dynamodb -> query($params);
			foreach($res['Items'] as $i){
				$tmp = $this -> marshaler -> unmarshalItem($i);
				$result[] = $tmp;
			}
			return $result;
		}catch(DynamoDbException $e){
			throw new Exception($e -> getMessage());
		}
	}
	
	
	
}