import { parse } from 'path';
import { createQuery } from './createQuery';

const mapToObject = (aMap) => {
  const obj = {};
  if (aMap) {
    aMap.forEach((v, k) => {
      obj[k] = v;
    });
  }
  return obj;
};

const queryToOdataString = (query): string => {
  let result = '';
  for (let key in query) {
    if (key.startsWith('$')) {
      if (result !== '') {
        result += '&';
      }
      result += `${key}=${query[key]}`;
    }
  }
  return result;
};

const processIncludes = (queryBuilder: any, odataQuery: any, alias: string, parent_metadata: any, navProps: string[] = []): [any, string] => {
  if (odataQuery.includes && odataQuery.includes.length > 0) {
    odataQuery.includes.forEach(item => {
      const relation_metadata = queryBuilder.connection.getMetadata(parent_metadata.relations.find(x => x.propertyPath === item.navigationProperty).type)
      const join = item.select === '*' ? 'leftJoinAndSelect' : 'leftJoin';
      if (join === 'leftJoin') {
        // add selections of data
        // todo: remove columns that are isSelect: false
        // queryBuilder.addSelect(item.select.split(',').map(x => x.trim()).filter(x => x !== ''));
      }

      const navProp = navProps.indexOf(item.navigationProperty) < 0 ? item.navigationProperty : item.alias;
      navProps.push(navProp);
      queryBuilder = queryBuilder[join](
        (alias ? alias + '.' : '') + item.navigationProperty,
        navProp,
        item.where.replace(/typeorm_query/g, item.navigationProperty),
        mapToObject(item.parameters)
      );

      if (item.orderby && item.orderby != '1') {
        const orders = item.orderby.split(',').map(i => i.trim().replace(/typeorm_query/g, item.navigationProperty));
        orders.forEach((itemOrd) => {
          queryBuilder = queryBuilder.addOrderBy(...(itemOrd.split(' ')));
        });
      }

      if (item.includes && item.includes.length > 0) {
        processIncludes(queryBuilder, { includes: item.includes }, item.navigationProperty, relation_metadata, navProps);
      }
    });
  }

  return queryBuilder;
};

const hasAnyFilter = (value: any) => {
  if (!value) {
    return;
  }

  if (value.left) {
    return hasAnyFilter(value.left.value) || hasAnyFilter(value.right.value);
  }

  if (value.raw?.indexOf('/any(') >= 0) {
    return value.raw;
  }

  return;
}

function parseCondition(input: string, paramIndex: number = 0, paramMap: any = {}) {
  const output = {
    tree: [],
    paramMap,
  };
  let lastIndex = 0;
  let nextIndex = 0;
  
  for (let i = 0; i < input.length; i++) {
    const index = i;
    if (index < nextIndex) {
      continue;
    }
    if (input.slice(index, index + 9) === "contains(") {
      lastIndex = index;
      nextIndex = findClosingParenthesisIndex(input, index + 8);
      
      const condition = {
        type: "condition",
        startIndex: index,
        endIndex: nextIndex,
        value: input.slice(index, nextIndex + 1),
        children: [],
        content: {},
      };
      condition.content = processRawCondition(condition.value, output.paramMap, paramIndex);
      paramIndex++;
      output.tree.push(condition);
      
      continue;
    }
    if (input.slice(index, index + 5) === " and " || input.slice(index, index + 4) === " or ") {
      let keyword = "and";
      let keywordLength = 5;
      if(input.slice(index, index + 4) === " or ") {
        keyword = "or";
        keywordLength = 4;
      }
      
      const condition = {
        type: "condition",
        startIndex: lastIndex,
        endIndex: index - 1,
        value: input.slice(lastIndex, index),
        children: [],
        content: {},
      };
      
      lastIndex = index;
      nextIndex = index + keywordLength;
      
      const operator = {
        type: "operator",
        startIndex: lastIndex,
        endIndex: nextIndex,
        value: input.slice(index, index + keywordLength),
        children: [],
      };
      if (output.tree.length === 0 || output.tree[output.tree.length - 1].type !== "group") {
        condition.content = processRawCondition(condition.value, paramMap, paramIndex);
        paramIndex++;
        output.tree.push(condition);
      }
      output.tree.push(operator);
      continue;
    }
    if (input[index] === "(") {
      lastIndex = index;
      nextIndex = findClosingParenthesisIndex(input, index);
      if (nextIndex !== -1) {
        const group = {
          type: "group",
          startIndex: index,
          endIndex: nextIndex,
          value: input.slice(index + 1, nextIndex),
          children: [],
        };
        const parsed = parseCondition(group.value, paramIndex++, paramMap);
        group.children = parsed.tree;
        output.tree.push(group);
        output.paramMap = parsed.paramMap;
      }
      continue;
    }
    
    if (index === input.length - 1 && input[index] !== ")") {
      const condition = {
        type: "condition",
        startIndex: nextIndex + 1,
        endIndex: index,
        value: input.slice(nextIndex, index + 1),
        children: [],
        content: {},
      };
      condition.content = processRawCondition(condition.value, paramMap, paramIndex);
      paramIndex++;
      output.tree.push(condition);
    }
  }
  return output;
}

function getWhereFromAnyString(string, metadata) {
  const splitString = string.split('/any(');
  const entity = splitString[0];
  
  const tableName = metadata.relations.find(c => c.propertyName === entity).entityMetadata.tableName;
  const childrenTableName = metadata.relations.find(c => c.propertyName === entity).inverseRelation.entityMetadata.tableName;
  const joinFieldName = metadata.relations.find(c => c.propertyName === entity).inverseSidePropertyPath;
  const targetName = metadata.relations.find(c => c.propertyName === entity).entityMetadata.targetName;

  
  let anyContent = splitString[1].slice(0, -1).split(":")[1];
  
  const parsed = parseCondition(anyContent);
  return {
    whereQueryString: getWhere(parsed.tree),
    tableName,
    childrenTableName,
    joinFieldName,
    targetName,
    paramMap: parsed.paramMap,
  };
}

function findClosingParenthesisIndex(expression, openIndex) {
  let stack = 0;
  
  // Start from the opening parenthesis index
  for (let i = openIndex; i < expression.length; i++) {
    const character = expression[i];
    // If it's an opening parenthesis, increment the stack
    if (character === '(') {
      stack++;
    } else if (character === ')') {
      // If it's a closing parenthesis, decrement the stack
      stack--;

      // If stack is zero, we've found the matching closing parenthesis
      if (stack === 0) {
        return i;
      }
    }
  }
  // If no closing parenthesis is found, return -1 or throw an error
  return -1;
}

function processRawCondition(rawCondition: string, paramMap: any, paramIndex: number = 0) {
  const paramName = "param_" + paramIndex;
  const conditionData = {
    operator: "eq",
    columnName: "",
    value: "",
    paramName,
  }
  
  const splitEqCondition = rawCondition.split(" eq ");
  if (splitEqCondition.length === 2) {
    conditionData.columnName = splitEqCondition[0].split('/')[1];
    conditionData.value = trimCommas(splitEqCondition[1]);
    paramMap[paramName] = conditionData.value;
    return conditionData;
  }
  
  const splitContainsCondition = rawCondition.split("contains");
  if (splitContainsCondition.length === 2) {
    conditionData.operator = "contains";
    const conditionContent = trimParanthesis(splitContainsCondition[1].replace("contains", ""));
    const splitConditionContent = conditionContent.split(',');
    conditionData.columnName = splitConditionContent[0].split('/')[1];
    conditionData.value = trimCommas(splitConditionContent[1].trim());
    paramMap[paramName] = "%" + conditionData.value + "%";
    return conditionData;
  }
  return { error: true };
}

function trimParanthesis(str) {
  if (str[0] === "(" && str[str.length - 1] === ")") {
    return str.slice(1, -1);
  }
  return str;
}

function trimCommas(str) {
  if ((str[0] === "'" && str[str.length - 1] === "'") || (str[0] === '"' && str[str.length - 1] === '"')) {
    return str.slice(1, -1);
  }
  return str;
}

function getWhere(conditionObject, prefix = "") {

  
  let where = "";
  for (let i in conditionObject) {
    const conditionComponent = conditionObject[i];
    if (conditionComponent.type === "condition" && conditionComponent.content) {
      const columnName = conditionComponent.content.columnName + prefix;
      const paramName = conditionComponent.content.paramName;
      if (conditionComponent.content.operator === "contains") {
        where += "`child`.`" + columnName + "` LIKE :" + paramName;
      }
      if (conditionComponent.content.operator === "eq") {
        where += "`child`.`" + columnName + "` = :" + paramName;
      }
    }
    if (conditionComponent.type === "operator") {
       where += conditionComponent.value;
    }
    if (conditionComponent.type === "group") {
       where += "(" + getWhere(conditionComponent.children) + ")";
    }
  }
  return where;
}

const executeQueryByQueryBuilder = async (inputQueryBuilder, query, options: any) => {
  const alias = inputQueryBuilder.expressionMap.mainAlias.name;
  //const filter = createFilter(query.$filter, {alias: alias});
  let odataQuery: any = {};
  if (query) {
    const odataString = queryToOdataString(query);
    if (odataString) {
      odataQuery = createQuery(odataString, { alias: alias });
    }
  }

  let queryBuilder = inputQueryBuilder;
  const metadata = inputQueryBuilder.connection.getMetadata(alias);
  let root_select = []

  // unlike the relations which are done via leftJoin[AndSelect](), we must explicitly add root
  // entity fields to the selection if it hasn't been narrowed down by the user.
  if (Object.keys(odataQuery).length === 0 || odataQuery.select === '*') {
    root_select = metadata.nonVirtualColumns.map(x => `${alias}.${x.propertyPath}`);
  } else {
    root_select = odataQuery.select.split(',').map(x => x.trim())
  }

  queryBuilder = queryBuilder.select(root_select);

  if (odataQuery.where) {
    let whereStringEnd = odataQuery.where.slice(-4);
    if (whereStringEnd.indexOf('AND') >= 0) {
      odataQuery.where = odataQuery.where.slice(0, -4);
    }
  }

  if (odataQuery.parameters.size > 0) {
    queryBuilder = queryBuilder
      .andWhere(odataQuery.where)
      .setParameters(mapToObject(odataQuery.parameters));
  }
    
  const filters = odataQuery.ast.value.options.find(o => o.type === 'Filter')?.value;
  const anyFilter = hasAnyFilter(filters?.value);
  if (anyFilter) {
    let anyFilterDetails = getWhereFromAnyString(anyFilter, metadata);
    if (anyFilterDetails.whereQueryString) { 
      queryBuilder = queryBuilder.andWhere(
        '`' + anyFilterDetails.targetName + '`.`id` IN ( ' +
        'SELECT distinct `parent`.`id` ' +
        'FROM `' + anyFilterDetails.tableName + '` AS parent ' +
        'JOIN `' + anyFilterDetails.childrenTableName + '` AS child ON `parent`.`id` = `child`.`' + anyFilterDetails.joinFieldName + 'Id` ' +
        'WHERE ' + anyFilterDetails.whereQueryString + ' ' +
    ')', anyFilterDetails.paramMap)
    }
  }

  queryBuilder = processIncludes(queryBuilder, odataQuery, alias, metadata);

  if (odataQuery.orderby && odataQuery.orderby !== '1') {
    const orders = odataQuery.orderby.split(',').map(i => i.trim());
    orders.forEach((item) => {
      queryBuilder = queryBuilder.addOrderBy(...(item.split(' ')));
    });
  }
  queryBuilder = queryBuilder.skip(query.$skip || 0);
  if (query.$top) {
    queryBuilder = queryBuilder.take(query.$top);
  }
  if (query.$count && query.$count !== 'false') {
    const resultData = await queryBuilder.getManyAndCount();
    return {
      items: resultData[0],
      count: resultData[1]
    }
  }

  return queryBuilder.getMany();
};

const changeDuplicateNavProps =(query: any, navProps: string[] = []) => {
  query.includes.forEach(item => {
    if (item.includes) {
      if (navProps.indexOf(item.navigationProperty) >= 0) {
        item.navigationProperty += '81';
      }
      navProps.push(item.navigationProperty);
      changeDuplicateNavProps(item, navProps);
    }
  });
  return query;
}

const executeQuery = async (repositoryOrQueryBuilder: any, query, options: any) => {
  options = options || {};
  const alias = options.alias || '';
  let queryBuilder = null;

  // check that input object is query builder
  if (typeof repositoryOrQueryBuilder.expressionMap !== 'undefined') {
    queryBuilder = repositoryOrQueryBuilder;
  } else {
    queryBuilder = repositoryOrQueryBuilder.createQueryBuilder(alias);
  }
  const result = await executeQueryByQueryBuilder(queryBuilder, query, { alias });
  return result;
};

export { executeQuery };