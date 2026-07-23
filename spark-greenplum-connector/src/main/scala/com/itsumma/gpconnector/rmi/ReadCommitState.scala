package com.itsumma.gpconnector.rmi

private[gpconnector] object ReadCommitState {
  def shouldFail(
      jobAborted: Boolean,
      coordinatorSqlComplete: Boolean,
      sqlTransferComplete: Boolean): Boolean =
    jobAborted || (coordinatorSqlComplete && !sqlTransferComplete)
}
