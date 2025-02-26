/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { IconNames } from '@blueprintjs/icons';
import { Timezone } from 'chronoshift';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';

import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import {
  isSimpleArray,
  prettyFormatIsoDateWithMsIfNeeded,
  toIsoStringInTimezone,
} from '../../utils';
import { ActionIcon } from '../action-icon/action-icon';

import './table-cell.scss';

const MAX_CHARS_TO_SHOW = 50;
const ABSOLUTE_MAX_CHARS_TO_SHOW = 5000;

interface ShortParts {
  prefix: string;
  omitted: string;
  suffix: string;
}

function shortenString(str: string): ShortParts {
  // Print something like:
  // BAAAArAAEiQKpDAEAACwZCBAGSBgiSEAAAAQpAIDwAg...23 omitted...gwiRoQBJIC
  const omit = str.length - (MAX_CHARS_TO_SHOW - 17);
  const prefix = str.slice(0, str.length - (omit + 10));
  const suffix = str.slice(str.length - 10);
  return {
    prefix,
    omitted: `...${omit} omitted...`,
    suffix,
  };
}

export interface TableCellProps {
  value: any;
  unlimited?: boolean;
  timezone?: Timezone;
}

export const TableCell = React.memo(function TableCell(props: TableCellProps) {
  const { value, unlimited, timezone = Timezone.UTC } = props;
  const [showValue, setShowValue] = useState<string | undefined>();

  function renderShowValueDialog() {
    if (!showValue) return;

    return <ShowValueDialog onClose={() => setShowValue(undefined)} str={showValue} />;
  }

  function renderTruncated(str: string) {
    if (str.length <= MAX_CHARS_TO_SHOW) {
      return <div className="table-cell plain">{str}</div>;
    }

    if (unlimited) {
      return (
        <div className="table-cell plain">
          {str.length < ABSOLUTE_MAX_CHARS_TO_SHOW
            ? str
            : `${str.slice(0, ABSOLUTE_MAX_CHARS_TO_SHOW)}...`}
        </div>
      );
    }

    const { prefix, omitted, suffix } = shortenString(str);
    return (
      <div className="table-cell truncated">
        {prefix}
        <span className="omitted">{omitted}</span>
        {suffix}
        <ActionIcon icon={IconNames.MORE} onClick={() => setShowValue(str)} />
        {renderShowValueDialog()}
      </div>
    );
  }

  if (value === '') {
    return <div className="table-cell empty">empty</div>;
  } else if (value == null) {
    return <div className="table-cell null">null</div>;
  } else if (value instanceof Date) {
    const dateValue = value.valueOf();
    return (
      <div className="table-cell timestamp" data-tooltip={String(dateValue)}>
        {isNaN(dateValue)
          ? 'Invalid date'
          : prettyFormatIsoDateWithMsIfNeeded(toIsoStringInTimezone(value, timezone))}
      </div>
    );
  } else if (isSimpleArray(value)) {
    return renderTruncated(`[${value.join(', ')}]`);
  } else if (typeof value === 'object') {
    return renderTruncated(JSONBig.stringify(value));
  } else {
    return renderTruncated(String(value));
  }
});
