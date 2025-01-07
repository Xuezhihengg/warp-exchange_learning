package org.warpexchange_learning.common;

public record ApiErrorResponse (ApiError error, String data, String message){
}
