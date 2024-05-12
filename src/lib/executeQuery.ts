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

const processIncludes = (queryBuilder: any, odataQuery: any, alias: string, parent_metadata: any): [any, string] => {
  if (odataQuery.includes && odataQuery.includes.length > 0) {
    odataQuery.includes.forEach(item => {
      const relation_metadata = queryBuilder.connection.getMetadata(parent_metadata.relations.find(x => x.propertyPath === item.navigationProperty).type)
      const join = item.select === '*' ? 'leftJoinAndSelect' : 'leftJoin';
      if (join === 'leftJoin') {
        // add selections of data
        // todo: remove columns that are isSelect: false
        // queryBuilder.addSelect(item.select.split(',').map(x => x.trim()).filter(x => x !== ''));
      }

      queryBuilder = queryBuilder[join](
        (alias ? alias + '.' : '') + item.navigationProperty,
        item.navigationProperty,
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
        processIncludes(queryBuilder, { includes: item.includes }, item.navigationProperty, relation_metadata);
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

const getAnyFilterDetails = (anyFilterString: string, metadata: any) => {
  const splitString = anyFilterString.split('/any(');
  const entity = splitString[0];
  const splitInfo = splitString[1].split('/')[1].split(' eq ');
  const field = splitInfo[0];
  let value = splitInfo[1].replace(')', '');

  if (value[0] === "'") {
    value = value.slice(1, -1);
  }

  if (entity && field && value !== undefined) {
    const tableName = metadata.relations.find(c => c.propertyName === entity).entityMetadata.tableName;
    const childrenTableName = metadata.relations.find(c => c.propertyName === entity).inverseRelation.entityMetadata.tableName;
    const joinFieldName = metadata.relations.find(c => c.propertyName === entity).inverseSidePropertyPath;
    const targetName = metadata.relations.find(c => c.propertyName === entity).entityMetadata.targetName;
    return {
      entity,
      field,
      value,
      targetName,
      tableName,
      childrenTableName,
      joinFieldName,
    }
  }
  return null;
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
    let anyFilterDetails = getAnyFilterDetails(anyFilter, metadata);
    if (anyFilterDetails) {
      queryBuilder = queryBuilder.andWhere(
        '`' + anyFilterDetails.targetName + '`.`id` IN ( ' +
        'SELECT distinct `parent`.`id` ' +
        'FROM `' + anyFilterDetails.tableName + '` AS parent ' +
        'JOIN `' + anyFilterDetails.childrenTableName + '` AS child ON `parent`.`id` = `child`.`' + anyFilterDetails.joinFieldName + 'Id` ' +
        'WHERE `child`.`' + anyFilterDetails.field + '` = :childPropValue ' +
    ')', 
      { childPropValue: anyFilterDetails.value })
      console.log(queryBuilder.getQuery());
      
    //   queryBuilder = queryBuilder.andWhere(
    //     '`RecordEntity`.`id` IN ( ' +
    //     'SELECT distinct `parent`.`id` ' +
    //     'FROM `record_entity` AS parent ' +
    //     'JOIN `record_entity` AS child ON `parent`.`id` = `child`.`parentId` ' +
    //     'WHERE `child`.`id` = :childId ' +
    // ')', 
    //   { childId: 52 })
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